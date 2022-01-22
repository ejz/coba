const utils = require('ejz-utils');
const crc32 = require('fast-crc32c');

const {Bitmap} = require('./Bitmap');
const {BitSliceIndex} = require('./BitSliceIndex');
const {MultipleBitmaps} = require('./MultipleBitmaps');

const {
    V2V,
    MAX,
    _TRUE,
    _FALSE,
    MIN_INT,
    MAX_INT,
    MIN_DATE,
    MAX_DATE,
    MIN_DATETIME,
    MAX_DATETIME,
    SHARDS,
    SEC_IN_A_DAY,
    MS_IN_A_DAY,
} = require('./constants');

function getReferences(ev) {
    let values = Object.create(null);
    let possible = [
        undefined,
        null,
        1,
        '',
        true,
        Symbol(),
        [],
        {},
    ];
    l: do {
        let re = utils.catchReferenceError(ev, values);
        if (re instanceof Error) {
            return null;
        }
        if (re == null) {
            return Object.keys(values);
        }
        for (let p of possible) {
            values[re] = p;
            let ret = utils.catchReferenceError(ev, values);
            if (ret instanceof Error) {
                continue;
            }
            if (ret == null || ret != re) {
                continue l;
            }
        }
        return null;
    } while (true); // eslint-disable-line
}

const OPTIONS = [
    'notnull',
    'precision',
    'min',
    'max',
    'parent',
    'child',
    'trackall',
    'isset',
    'lowrank',
    'values',
    'evaluate',
];

function toNumberValue(val, min, max, precision) {
    if (isNaN(val)) {
        return null;
    }
    if (precision) {
        val = utils.floor(val, precision);
    }
    if (min <= val && val <= max) {
        return val;
    }
    return null;
}

function toDateValue(val, min, max) {
    let unix = utils.date2unix(val);
    if (unix == null) {
        return null;
    }
    unix = Math.floor(unix / SEC_IN_A_DAY);
    if (min <= unix && unix <= max) {
        return utils.date(new Date(unix * MS_IN_A_DAY));
    }
    return null;
}

function toDateInt(val) {
    return Math.floor(utils.date2unix(val) / SEC_IN_A_DAY);
}

function fromDateInt(val) {
    return utils.date(new Date(val * MS_IN_A_DAY));
}

function toDatetimeValue(val, min, max) {
    let unix = utils.date2unix(val);
    if (unix == null) {
        return null;
    }
    if (min <= unix && unix <= max) {
        return utils.datetime(new Date(unix * 1E3));
    }
    return null;
}

function toDatetimeInt(val) {
    return utils.date2unix(val);
}

function fromDatetimeInt(val) {
    return utils.datetime(new Date(val * 1E3));
}

function compose(proto, ...changes) {
    let obj = Object.create(proto);
    changes.forEach((changes) => {
        obj = Object.assign(obj, changes);
    });
    return obj;
}

function _f(...args) {
    return args.every((arg) => typeof(arg) == 'function');
}

function _GetSetter(key) {
    return function(field, id, oldv, newv) {
        let old = oldv != null || oldv === undefined;
        oldv = oldv ?? null;
        let ref = field[key];
        if (newv == null) {
            if (old) {
                ref.remove(id, oldv);
            }
            return;
        }
        if (!old) {
            ref.add(id, newv);
            return;
        }
        if (
            oldv == null &&
            (ref.all || ref.bitmaps.all) &&
            !ref.externalAll &&
            !ref.has(id)
        ) {
            ref.add(id, newv);
            return;
        }
        ref.update(id, oldv, newv);
    };
}

function _GetGetter(key) {
    return function(field, id) {
        return field[key].getBitValue(id);
    };
}

function _GetSyncer(key) {
    return function(field) {
        field[key].sync();
    };
}

const _Base = compose(null, {
    toString() {
        return this.name;
    },
    isSortable(...args) {
        return !this.isDisk(...args);
    },
    isSearchable(...args) {
        return this.isIndex(...args);
    },
    isDisk(...args) {
        return _f(this.serializeToDisk, this.deserializeFromDisk) && !this.isSecondary(...args);
    },
    isIndex() {
        return _f(this.serializeToIndex);
    },
    isDiskIndex(...args) {
        return this.isDisk(...args) && this.isIndex(...args);
    },
    isDiskIndexRequire(...args) {
        return this.isDiskIndex(...args) && this.isRequire(...args);
    },
    acceptRange() {
        return _f(this.resolveRange);
    },
    acceptKeys() {
        return _f(this.resolveKeys);
    },
    acceptAll() {
        return _f(this.resolveAll);
    },
    isFulltext() {
        return String(this) == 'Fulltext';
    },
    isParent() {
        return String(this) == 'Parent';
    },
    isCounter() {
        return String(this) == 'Counter';
    },
    isRequire(...args) {
        return this.isFulltext(...args) || (_f(this.checkIsRequire) && this.checkIsRequire(...args));
    },
    isArray(...args) {
        return String(this).includes('Array') || this.isSet(...args);
    },
    isSet() {
        return String(this).includes('Set');
    },
    isSecondary({options: {_evaluate: e}}) {
        return e && e.expression != null && e.fields != null;
    },
    validateOptions(options) {
        let diff = utils.diff(Object.keys(options), OPTIONS);
        if (diff.length) {
            throw new TypesInvalidOptionsError(diff.shift());
        }
        for (let option of OPTIONS) {
            let restrict = this['restrict_' + option] ?? false;
            let def = this['default_' + option] ?? null;
            let func = this['validate_' + option] ?? null;
            let opt = options[option] ?? null;
            if (def == null && opt == null) {
                continue;
            }
            if ((def == null || restrict) && opt != null) {
                throw new TypesInvalidOptionsError(option);
            }
            opt = opt ?? def;
            func = func ? func.bind(this) : V2V;
            options[option] = opt;
            options['_' + option] = func(opt, options);
            if (options['_' + option] == null) {
                throw new TypesInvalidOptionsError(option);
            }
        }
        return options;
    },
    default_notnull: false,
    validate_notnull(val) {
        return typeof(val) == 'boolean' ? val : null;
    },
    default_evaluate: '',
    validate_evaluate(val) {
        val = val.trim();
        if (val == '') {
            return val;
        }
        let refs = getReferences(val);
        if (!refs.length) {
            return null;
        }
        return {expression: val, fields: refs};
    },
});

const _Boolean = compose(_Base, {
    name: 'Boolean',
    fromExternal(val) {
        if (typeof(val) != 'boolean') {
            return null;
        }
        return val;
    },
    fromLiteral(val) {
        val = val.toLowerCase();
        if (_TRUE.includes(val)) {
            return true;
        }
        if (_FALSE.includes(val)) {
            return false;
        }
        return null;
    },
    serializeToDisk(val) {
        return val ? 1 : 0;
    },
    deserializeFromDisk(val) {
        return Boolean(val);
    },
});

exports.Boolean = _Boolean;

const _String = compose(_Base, {
    name: 'String',
    fromExternal(val, {_values}) {
        if (typeof(val) != 'string') {
            return null;
        }
        if (!_values) {
            return val;
        }
        val = val.toUpperCase();
        let idx = _values.indexOf(val);
        return ~idx ? val : null;
    },
    fromLiteral: V2V,
    serializeToDisk: V2V,
    deserializeFromDisk: V2V,
});

exports.String = _String;

const _Enum = compose(_Base, {
    name: 'Enum',
    fromExternal: _String.fromExternal,
    fromLiteral: _String.fromExternal,
    serializeToDisk(val, {_values}) {
        return _values.indexOf(val);
    },
    deserializeFromDisk(val, {_values}) {
        return _values[val];
    },
    default_values: [],
    validate_values(values) {
        values = values.map((v) => v.toUpperCase().trim());
        values = utils.unique(values);
        values = values.filter((v) => v != '');
        return values.length ? values : null;
    },
});

exports.Enum = _Enum;

const _Number = compose(_Base, {
    name: 'Number',
    fromExternal(val, {_min, _max, _precision}) {
        if (typeof(val) != 'number') {
            return null;
        }
        return toNumberValue(val, _min, _max, _precision);
    },
    fromLiteral(val, {_min, _max, _precision}) {
        return toNumberValue(+val, _min, _max, _precision);
    },
    serializeToDisk: V2V,
    deserializeFromDisk: V2V,
    default_precision: 0,
    validate_precision(v) {
        return toNumberValue(v, 0, 6, 0);
    },
    default_min: String(MIN_INT),
    validate_min(v, {_precision}) {
        return toNumberValue(+v, MIN_INT, MAX_INT, _precision);
    },
    default_max: String(MAX_INT),
    validate_max(v, {_precision, _min}) {
        return toNumberValue(+v, Math.max(MIN_INT, _min), MAX_INT, _precision);
    },
});

exports.Number = _Number;

const _Date = compose(_Base, {
    name: 'Date',
    fromExternal(val, {_min, _max}) {
        if (val instanceof Date) {
            val = String(val);
        }
        if (typeof(val) != 'string') {
            return null;
        }
        return toDateValue(val, _min, _max);
    },
    fromLiteral(val, {_min, _max}) {
        return toDateValue(val, _min, _max);
    },
    serializeToDisk(val) {
        return toDateInt(val);
    },
    deserializeFromDisk(val) {
        return fromDateInt(val);
    },
    default_min: MIN_DATE,
    validate_min(v) {
        return toDateInt(toDateValue(v, 0, MAX));
    },
    default_max: MAX_DATE,
    validate_max(v, {_min}) {
        return toDateInt(toDateValue(v, Math.max(0, _min), MAX));
    },
});

exports.Date = _Date;

const _Datetime = compose(_Base, {
    name: 'Datetime',
    fromExternal(val, {_min, _max}) {
        if (val instanceof Date) {
            val = String(val);
        }
        if (typeof(val) != 'string') {
            return null;
        }
        return toDatetimeValue(val, _min, _max);
    },
    fromLiteral(val, {_min, _max}) {
        return toDatetimeValue(val, _min, _max);
    },
    serializeToDisk(val) {
        return toDatetimeInt(val);
    },
    deserializeFromDisk(val) {
        return fromDatetimeInt(val);
    },
    default_min: MIN_DATETIME,
    validate_min(v) {
        return toDatetimeInt(toDatetimeValue(v, 0, MAX));
    },
    default_max: MAX_DATETIME,
    validate_max(v, {_min}) {
        return toDatetimeInt(toDatetimeValue(v, Math.max(0, _min), MAX));
    },
});

exports.Datetime = _Datetime;

const _Object = compose(_Base, {
    name: 'Object',
    fromExternal(val) {
        if (val == null || typeof(val) != 'object') {
            return null;
        }
        return val;
    },
    serializeToDisk: V2V,
    deserializeFromDisk: V2V,
    resolveKeys(field, obj, ...keys) {
        while (obj != null && keys.length) {
            obj = obj[keys.shift()];
        }
        return obj;
    },
});

exports.Object = _Object;

const _BooleanArray = compose(_Base, {
    name: 'BooleanArray',
    fromExternal(val) {
        val = Array.isArray(val) ? val : [val];
        val = val.map((v) => _Boolean.fromExternal(v));
        if (~val.findIndex((v) => v == null)) {
            return null;
        }
        return val;
    },
    serializeToDisk(val) {
        return val.map((v) => v ? 1 : 0).join(',');
    },
    deserializeFromDisk(val) {
        if (val == '') {
            return [];
        }
        return val.split(',').map((v) => +v ? true : false);
    },
});

exports.BooleanArray = _BooleanArray;

const _StringArray = compose(_Base, {
    name: 'StringArray',
    fromExternal(val, options) {
        val = Array.isArray(val) ? val : [val];
        val = val.map((v) => _String.fromExternal(v, options));
        if (~val.findIndex((v) => v == null)) {
            return null;
        }
        if (options.isset) {
            val = utils.unique(val);
        }
        return val;
    },
    serializeToDisk: V2V,
    deserializeFromDisk: V2V,
});

exports.StringArray = _StringArray;

const _EnumArray = compose(_Base, {
    name: 'EnumArray',
    fromExternal(val, options) {
        val = Array.isArray(val) ? val : [val];
        val = val.map((v) => _Enum.fromExternal(v, options));
        if (~val.findIndex((v) => v == null)) {
            return null;
        }
        if (options.isset) {
            val = utils.unique(val);
        }
        return val;
    },
    serializeToDisk(val, options) {
        return val.map((v) => _Enum.serializeToDisk(v, options));
    },
    deserializeFromDisk(val, options) {
        return val.map((v) => _Enum.deserializeFromDisk(v, options));
    },
});

exports.EnumArray = _EnumArray;

const _NumberArray = compose(_Base, {
    name: 'NumberArray',
    fromExternal(val, options) {
        val = Array.isArray(val) ? val : [val];
        val = val.map((v) => _Number.fromExternal(v, options));
        if (~val.findIndex((v) => v == null)) {
            return null;
        }
        if (options.isset) {
            val = utils.unique(val);
        }
        return val;
    },
    serializeToDisk: V2V,
    deserializeFromDisk: V2V,
    default_precision: _Number.default_precision,
    validate_precision: _Number.validate_precision,
    default_min: _Number.default_min,
    validate_min: _Number.validate_min,
    default_max: _Number.default_max,
    validate_max: _Number.validate_max,
});

exports.NumberArray = _NumberArray;

const _DateArray = compose(_Base, {
    name: 'DateArray',
    fromExternal(val, options) {
        val = Array.isArray(val) ? val : [val];
        val = val.map((v) => _Date.fromExternal(v, options));
        if (~val.findIndex((v) => v == null)) {
            return null;
        }
        if (options.isset) {
            val = utils.unique(val);
        }
        return val;
    },
    serializeToDisk(val) {
        return val.map((v) => toDateInt(v));
    },
    deserializeFromDisk(val) {
        return val.map((v) => fromDateInt(v));
    },
    default_min: _Date.default_min,
    validate_min: _Date.validate_min,
    default_max: _Date.default_max,
    validate_max: _Date.validate_max,
});

exports.DateArray = _DateArray;

const _DatetimeArray = compose(_Base, {
    name: 'DatetimeArray',
    fromExternal(val, options) {
        val = Array.isArray(val) ? val : [val];
        val = val.map((v) => _Datetime.fromExternal(v, options));
        if (~val.findIndex((v) => v == null)) {
            return null;
        }
        if (options.isset) {
            val = utils.unique(val);
        }
        return val;
    },
    serializeToDisk(val) {
        return val.map((v) => toDatetimeInt(v));
    },
    deserializeFromDisk(val) {
        return val.map((v) => fromDatetimeInt(v));
    },
    default_min: _Datetime.default_min,
    validate_min: _Datetime.validate_min,
    default_max: _Datetime.default_max,
    validate_max: _Datetime.validate_max,
});

exports.DatetimeArray = _DatetimeArray;

const _StringSet = compose(_StringArray, {
    name: 'StringSet',
    restrict_isset: true,
    default_isset: true,
});

exports.StringSet = _StringSet;

const _EnumSet = compose(_EnumArray, {
    name: 'EnumSet',
    restrict_isset: true,
    default_isset: true,
});

exports.EnumSet = _EnumSet;

const _NumberSet = compose(_NumberArray, {
    name: 'NumberSet',
    restrict_isset: true,
    default_isset: true,
});

exports.NumberSet = _NumberSet;

const _DateSet = compose(_DateArray, {
    name: 'DateSet',
    restrict_isset: true,
    default_isset: true,
});

exports.DateSet = _DateSet;

const _DatetimeSet = compose(_DatetimeArray, {
    name: 'DatetimeSet',
    restrict_isset: true,
    default_isset: true,
});

exports.DatetimeSet = _DatetimeSet;

const _Id = compose(_Base, {
    name: 'Id',
    fromLiteral: getIdFromLiteral,
    serializeToIndex: V2V,
    resolveRange(field, from, to) {
        return Bitmap.range(field.all, from, to);
    },
    resolveAll(field) {
        return field.all;
    },
});

exports.Id = _Id;

const _BaseBitSliceIndex = compose(_Base, {
    _create(field, max) {
        if (field.folder != null) {
            utils.mkdir(field.folder);
        }
        field.bsi = new BitSliceIndex({
            max,
            folder: field.folder,
            all: field.isNotNull ? field.all : null,
        });
    },
    set: _GetSetter('bsi'),
    get: _GetGetter('bsi'),
    sync: _GetSyncer('bsi'),
    resolveAll(field) {
        return field.bsi.bitmaps.all;
    },
    resolveRange(field, from, to) {
        return field.bsi.getBitmap(from, to);
    },
    resolveValue(field, val) {
        return field.bsi.getBitmap(val, val);
    },
    sort(field, bitmap, asc) {
        return field.bsi.sort(bitmap, asc);
    },
});

const _BooleanIndex = compose(_BaseBitSliceIndex, {
    name: 'BooleanIndex',
    fromExternal: _Boolean.fromExternal,
    fromLiteral: _Boolean.fromLiteral,
    serializeToIndex: _Boolean.serializeToDisk,
    deserializeFromIndex: _Boolean.deserializeFromDisk,
    create(field) {
        this._create(field, 1);
    },
});

exports.BooleanIndex = _BooleanIndex;

const _StringIndex = compose(_BaseBitSliceIndex, {
    name: 'StringIndex',
    fromExternal: _String.fromExternal,
    fromLiteral: _String.fromLiteral,
    serializeToDisk: _String.serializeToDisk,
    deserializeFromDisk: _String.deserializeFromDisk,
    serializeToIndex(val) {
        return crc32.calculate(val);
    },
    create(field) {
        this._create(field, MAX);
    },
    resolveRange: null,
});

exports.StringIndex = _StringIndex;

const _EnumIndex = compose(_BaseBitSliceIndex, {
    name: 'EnumIndex',
    fromExternal: _Enum.fromExternal,
    fromLiteral: _Enum.fromLiteral,
    serializeToIndex: _Enum.serializeToDisk,
    deserializeFromIndex: _Enum.deserializeFromDisk,
    create(field) {
        let l = field.options._values.length;
        this._create(field, l - 1);
    },
    resolveRange: null,
    default_values: _Enum.default_values,
    validate_values: _Enum.validate_values,
});

exports.EnumIndex = _EnumIndex;

const _NumberIndex = compose(_BaseBitSliceIndex, {
    name: 'NumberIndex',
    fromExternal: _Number.fromExternal,
    fromLiteral: _Number.fromLiteral,
    serializeToIndex(val, {_min, _precision}) {
        let mult = Math.pow(10, _precision);
        return Math.floor((val - _min) * mult);
    },
    deserializeFromIndex(val, {_min, _precision}) {
        let mult = Math.pow(10, _precision);
        return utils.floor((val / mult) + _min, 2);
    },
    create(field) {
        let {_min, _max, _precision} = field.options;
        let mult = Math.pow(10, _precision);
        this._create(field, Math.floor((_max - _min) * mult));
    },
    default_precision: _Number.default_precision,
    validate_precision: _Number.validate_precision,
    default_min: String(0),
    validate_min: _Number.validate_min,
    default_max: String(MAX),
    validate_max: _Number.validate_max,
});

exports.NumberIndex = _NumberIndex;

const _Parent = compose(_BaseBitSliceIndex, {
    name: 'Parent',
    fromExternal: getIdFromLiteral,
    fromLiteral: getIdFromLiteral,
    serializeToIndex: V2V,
    deserializeFromIndex: V2V,
    create(field) {
        this._create(field, MAX);
    },
    default_parent: '',
    validate_parent(p) {
        if (typeof(p) == 'string' && p != '') {
            return p;
        }
        return null;
    },
    id2fk(field, bitmap) {
        return field.bsi.remapBitmap(bitmap, false);
    },
    fk2id(field, bitmap) {
        return field.bsi.remapBitmap(bitmap, true);
    },
});

exports.Parent = _Parent;

const _Counter = compose(_BaseBitSliceIndex, {
    name: 'Counter',
    fromLiteral(val) {
        return toNumberValue(+val, 0, MAX);
    },
    serializeToIndex: V2V,
    deserializeFromIndex: V2V,
    create(field) {
        this._create(field, MAX);
    },
    default_child: '',
    validate_child(c) {
        if (typeof(c) == 'string' && c != '') {
            return c;
        }
        return null;
    },
    increment(field, v) {
        field.bsi.increment(v, +1);
    },
    decrement(field, v) {
        field.bsi.increment(v, -1);
    },
    restrict_notnull: true,
    default_notnull: true,
});

exports.Counter = _Counter;

const _BaseBitmapIndex = compose(_Base, {
    create(field) {
        if (field.folder != null) {
            utils.mkdir(field.folder);
        }
        let ta = !!field.options._trackall;
        field.multipleBitmaps = new MultipleBitmaps({
            folder: field.folder,
            all: field.isNotNull ? field.all : ta,
        });
    },
    set: _GetSetter('multipleBitmaps'),
    get: _GetGetter('multipleBitmaps'),
    sync: _GetSyncer('multipleBitmaps'),
    resolveAll(field) {
        return field.multipleBitmaps.all;
    },
    acceptAll(field) {
        let ta = !!field.options._trackall;
        return field.isNotNull || ta;
    },
    resolveValue(field, val) {
        return field.multipleBitmaps.getBitmap(val);
    },
});

const _NumberArrayIndex = compose(_BaseBitmapIndex, {
    name: 'NumberArrayIndex',
    fromExternal: _NumberArray.fromExternal,
    fromLiteral: _Number.fromLiteral,
    serializeToDisk: V2V,
    deserializeFromDisk: V2V,
    serializeToIndex: V2V,
    default_precision: _Number.default_precision,
    validate_precision: _Number.validate_precision,
    default_min: _Number.default_min,
    validate_min: _Number.validate_min,
    default_max: _Number.default_max,
    validate_max: _Number.validate_max,
    default_trackall: false,
    checkIsRequire({options: {_min, _max, _precision}}) {
        let rank = (_max - _min) * Math.pow(10, _precision);
        return rank > 1E3;
    },
});

exports.NumberArrayIndex = _NumberArrayIndex;

const _NumberSetIndex = compose(_NumberArrayIndex, {
    name: 'NumberSetIndex',
    restrict_isset: true,
    default_isset: true,
    serializeToDisk: null,
    deserializeFromDisk: null,
    deserializeFromIndex(val) {
        return val.map((v) => +v);
    },
});

exports.NumberSetIndex = _NumberSetIndex;

const _StringArrayIndex = compose(_BaseBitmapIndex, {
    name: 'StringArrayIndex',
    fromExternal: _StringArray.fromExternal,
    fromLiteral: _String.fromLiteral,
    serializeToDisk: V2V,
    deserializeFromDisk: V2V,
    serializeToIndex: V2V,
    default_lowrank: false,
    checkIsRequire({options: {_lowrank}}) {
        return !_lowrank;
    },
});

exports.StringArrayIndex = _StringArrayIndex;

const _EnumArrayIndex = compose(_BaseBitmapIndex, {
    name: 'EnumArrayIndex',
    fromExternal: _EnumArray.fromExternal,
    fromLiteral: _Enum.fromLiteral,
    serializeToDisk: _EnumArray.serializeToDisk,
    deserializeFromDisk: _EnumArray.deserializeFromDisk,
    serializeToIndex: V2V,
    default_values: _Enum.default_values,
    validate_values: _Enum.validate_values,
});

exports.EnumArrayIndex = _EnumArrayIndex;

const _StringSetIndex = compose(_StringArrayIndex, {
    name: 'StringSetIndex',
    restrict_isset: true,
    default_isset: true,
    serializeToDisk: null,
    deserializeFromDisk: null,
    deserializeFromIndex: V2V,
});

exports.StringSetIndex = _StringSetIndex;

const _EnumSetIndex = compose(_EnumArrayIndex, {
    name: 'EnumSetIndex',
    restrict_isset: true,
    default_isset: true,
    serializeToDisk: null,
    deserializeFromDisk: null,
    deserializeFromIndex: V2V,
});

exports.EnumSetIndex = _EnumSetIndex;

const _Fulltext = compose(_BaseBitmapIndex, {
    name: 'Fulltext',
    fromExternal: _String.fromExternal,
    fromLiteral: _String.fromLiteral,
    serializeToDisk: _String.serializeToDisk,
    deserializeFromDisk: _String.deserializeFromDisk,
    serializeToIndex: getFulltextValue,
    resolveValue(field, val) {
        let resolve = (val) => {
            if (typeof(val) == 'string') {
                return field.multipleBitmaps.bitmaps[val];
            }
            if (!val.length) {
                return new Bitmap();
            }
            let call = 'and';
            if (val[0] == '|') {
                call = 'or';
                val = val.slice(1);
            }
            return Bitmap[call](...val.map(resolve));
        };
        return resolve(val);
    },
    default_trackall: false,
});

exports.Fulltext = _Fulltext;

function getIdFromLiteral(val) {
    let v = +val;
    if (isNaN(v) || v < 1 || MAX < v) {
        return null;
    }
    val = String(val);
    if (!/^\d+$/.test(val) || /^0/.test(val)) {
        return null;
    }
    return v;
}

exports.getIdFromLiteral = getIdFromLiteral;

function getFulltextValue(fulltext) {
    if (utils.isObject(fulltext)) {
        return getFulltextValueComplex(fulltext);
    }
    let words = normalizeText(fulltext);
    let sl = SHARDS.length;
    let ret = [];
    words.forEach(({word}) => {
        let shard = SHARDS[crc32.calculate(word) % sl];
        word = '$' + word + '$';
        let l = word.length;
        for (let i = 0; i < l - 2; i++) {
            ret.push(shard + word.substring(i, i + 3));
        }
    });
    return ret;
}

exports.getFulltextValue = getFulltextValue;

function getFulltextValueComplex({value, allowPrefix, allowPostfix, placeholder, field}) {
    allowPrefix = allowPrefix ?? false;
    allowPostfix = allowPostfix ?? false;
    if (placeholder != null) {
        return getFulltextValuePlaceholder({value, allowPrefix, allowPostfix, placeholder, field});
    }
    let words = normalizeText(value, {allowPrefix, allowPostfix});
    let sl = SHARDS.length;
    let and = [];
    for (let {allowPrefix, word, allowPostfix} of words) {
        let possibleShards = (allowPrefix || allowPostfix) ? SHARDS : [SHARDS[crc32.calculate(word) % sl]];
        word = (allowPrefix ? '' : '$') + word + (allowPostfix ? '' : '$');
        let possibleWords = word.length < 3 ? genWords(word) : [word];
        let found = possibleShards.map((shard) => {
            let found = possibleWords.map((word) => {
                return searchWordOnShard(word, shard, field);
            }).filter(Boolean);
            if (!found.length) {
                return;
            }
            found.unshift('|');
            return found;
        }).filter(Boolean);
        if (!found.length) {
            return [];
        }
        found.unshift('|');
        and.push(found);
    }
    return and;
}

exports.getFulltextValueComplex = getFulltextValueComplex;

function searchWordOnShard(word, shard, field) {
    let l = word.length;
    let ret = [];
    for (let i = 0; i < l - 2; i++) {
        let w = word.substring(i, i + 3);
        let s = shard + w;
        if (!field.multipleBitmaps.bitmaps[s]) {
            return;
        }
        ret.push(s);
    }
    return ret;
}

exports.searchWordOnShard = searchWordOnShard;

function getFulltextValuePlaceholder({value, allowPrefix, allowPostfix, placeholder, field}) {
    let words = normalizeTexts(value, {allowPrefix, allowPostfix, placeholder});
    let sl = SHARDS.length;
    let and = [];
    for (let {allowPrefix, word, allowPostfix, placeholder} of words) {
        let possibleShards = (allowPrefix || allowPostfix || placeholder != null) ? SHARDS : [SHARDS[crc32.calculate(word) % sl]];
        word = (allowPrefix ? '' : '$') + word + (allowPostfix ? '' : '$');
        let possibleWords = word.length < 3 ? genWords(word) : [word];
        let found = possibleShards.map((shard) => {
            let found = possibleWords.map((word) => {
                if (placeholder == null) {
                    return searchWordOnShard(word, shard, field);
                }
                let words = word.split(placeholder);
                let and = [];
                for (let word of words) {
                    if (word.length < 3) {
                        let _words = genWords(word).map((word) => {
                            return searchWordOnShard(word, shard, field);
                        }).filter(Boolean);
                        if (!_words.length) {
                            return;
                        }
                        _words.unshift('|');
                        and.push(_words);
                        continue;
                    }
                    let found = searchWordOnShard(word, shard, field);
                    if (!found) {
                        return;
                    }
                    and.push(found);
                }
                return and;
            }).filter(Boolean);
            if (!found.length) {
                return;
            }
            found.unshift('|');
            return found;
        }).filter(Boolean);
        if (!found.length) {
            return [];
        }
        found.unshift('|');
        and.push(found);
    }
    return and;
}

exports.getFulltextValuePlaceholder = getFulltextValuePlaceholder;

function normalizeText(text, extra) {
    extra = extra ?? {};
    let {allowPrefix, allowPostfix} = extra;
    allowPrefix = allowPrefix ?? false;
    allowPostfix = allowPostfix ?? false;
    let startsWithDelimiter = /^\s+/.test(text);
    let endsWithDelimiter = /\s+$/.test(text);
    if (startsWithDelimiter || endsWithDelimiter) {
        text = text.replace(/^\s+|\s+$/g, '');
    }
    text = text.replace(/([a-zA-Z0-9])[']([a-zA-Z0-9])/g, '$1$2');
    text = text.replace(/[^a-zA-Z0-9]+/g, ' ');
    let startsWithSpace = text.startsWith(' ');
    let endsWithSpace = text.endsWith(' ');
    if (startsWithSpace || endsWithSpace) {
        text = text.trim();
    }
    text = text.toLowerCase();
    if (text == '') {
        return [];
    }
    let words = text.split(' ');
    allowPrefix = allowPrefix && !startsWithDelimiter && !startsWithSpace;
    allowPostfix = allowPostfix && !endsWithDelimiter && !endsWithSpace;
    if (words.length == 1) {
        return [{word: words[0], allowPrefix, allowPostfix}];
    }
    let first = allowPrefix ? words.shift() : null;
    let last = allowPostfix ? words.pop() : null;
    words = suniq(words);
    if (first != null && words.includes(first)) {
        allowPrefix = false;
    }
    if (last != null && words.includes(last)) {
        allowPostfix = false;
    }
    if (allowPrefix && first != null) {
        words.unshift(first);
    }
    if (allowPostfix && last != null) {
        words.push(last);
    }
    if (allowPrefix && allowPostfix && words.length == 2 && words[0] == words[1]) {
        words.pop();
    }
    let l = words.length;
    return words.map((word, i) => ({
        word,
        allowPrefix: allowPrefix && (i == 0),
        allowPostfix: allowPostfix && (i == l - 1),
    }));
}

exports.normalizeText = normalizeText;

function normalizeTexts(texts, extra) {
    extra = extra ?? {};
    let {allowPrefix, allowPostfix, placeholder} = extra;
    allowPrefix = allowPrefix ?? false;
    allowPostfix = allowPostfix ?? false;
    placeholder = placeholder ?? '*';
    texts = texts.split(placeholder);
    let collect = [];
    let l = texts.length;
    let last;
    for (let i = 0; i < l; i++) {
        let isFirst = i == 0;
        let isLast = i == l - 1;
        let text = texts[i];
        let words = normalizeText(text, {
            allowPrefix: !isFirst || allowPrefix,
            allowPostfix: !isLast || allowPostfix,
        });
        for (let {word, allowPrefix, allowPostfix} of words) {
            if (last != null && last.allowPostfix && allowPrefix) {
                last.allowPostfix = allowPostfix;
                last.placeholder = placeholder;
                last.word += placeholder + word;
            } else {
                last = {word, allowPrefix, allowPostfix};
                collect.push(last);
            }
        }
    }
    return collect;
}

exports.normalizeTexts = normalizeTexts;

const chars = '0123456789abcdefghijklmnopqrstuvwxyz$'.split('');

function genWords(word) {
    if (word.startsWith('$')) {
        return chars.map((char) => word + char);
    }
    if (word.endsWith('$')) {
        return chars.map((char) => char + word);
    }
    if (word.length == 2) {
        let sameChars = word[0] == word[1];
        let words = chars.map((char) => char + word);
        if (!sameChars) {
            words.push(...chars.map((char) => word + char));
            return words;
        }
        words.push(...chars.filter((c) => c != word[0]).map((char) => word + char));
        return words;
    }
    let ret = [];
    let collect = {};
    let forEach = (w) => {
        if (!collect[w]) {
            collect[w] = true;
            ret.push(w);
        }
    };
    for (let char of chars) {
        genWords(word + char).forEach(forEach);
        if (word != char) {
            genWords(char + word).forEach(forEach);
        }
    }
    return ret;
}

exports.genWords = genWords;

function suniq(array) {
    return array.reduce((a, c) => {
        if (!a[0][c]) {
            a[0][c] = true;
            a[1].push(c);
        }
        return a;
    }, [{}, []]).pop();
}

exports.suniq = suniq;

class TypesError extends Error {}

exports.TypesError = TypesError;

class TypesInvalidOptionsError extends TypesError {}

exports.TypesInvalidOptionsError = TypesInvalidOptionsError;

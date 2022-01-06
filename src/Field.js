const utils = require('ejz-utils');

const {Bitmap} = require('./Bitmap');
const types = require('./types');

class Field {
    constructor(name, options) {
        this.name = name;
        options = options ?? {};
        this.type = options.type;
        delete options.type;
        if (!types[this.type]) {
            throw new FieldInvalidTypeError(this.name);
        }
        this.type = types[this.type];
        this.folder = options.folder ?? null;
        delete options.folder;
        this.all = options.all ?? null;
        delete options.all;
        if (this.all != null) {
            utils.ok.instance(Bitmap, this.all);
        }
        this.options = this.type.validateOptions(options);
        this.isNotNull = this.options._notnull;
        this.isSortable = this.type.isSortable(this);
        this.isSearchable = this.type.isSearchable(this);
        this.isDisk = this.type.isDisk(this);
        this.isIndex = this.type.isIndex(this);
        this.isParent = this.type.isParent(this);
        this.isCounter = this.type.isCounter(this);
        this.isArray = this.type.isArray(this);
        this.isSet = this.type.isSet(this);
        this.isFulltext = this.type.isFulltext(this);
        this.isDiskIndex = this.type.isDiskIndex(this);
        this.acceptKeys = this.type.acceptKeys(this);
        this.isDiskIndexRequire = this.type.isDiskIndexRequire(this);
        this.isSecondary = this.type.isSecondary(this);
        this.acceptRange = this.type.acceptRange(this);
        this.acceptAll = this.type.acceptAll(this);
        if (this.type.create) {
            this.type.create(this);
        }
    }

    toString() {
        return this.name;
    }

    sync() {
        if (this.type.sync) {
            this.type.sync(this);
        }
    }
    
    serializeToDisk(value) {
        return this.type.serializeToDisk(value, this.options);
    }

    deserializeFromDisk(value) {
        return this.type.deserializeFromDisk(value, this.options);
    }

    serializeToIndex(value) {
        return this.type.serializeToIndex(value, this.options);
    }

    deserializeFromIndex(value) {
        return this.type.deserializeFromIndex(value, this.options);
    }

    fromExternal(value) {
        return this.type.fromExternal(value, this.options);
    }

    fromLiteral(value) {
        return this.type.fromLiteral(value, this.options);
    }

    get(id, deserialize) {
        let val = this.type.get(this, id);
        if (val == null) {
            return null;
        }
        if (deserialize ?? true) {
            val = this.deserializeFromIndex(val);
        }
        return val;
    }

    set(id, oldv, newv) {
        oldv = oldv != null ? this.serializeToIndex(oldv) : oldv;
        newv = newv != null ? this.serializeToIndex(newv) : newv;
        this.type.set(this, id, oldv, newv);
    }

    resolveRange([exc1, val1, val2, exc2]) {
        if (val1 != null) {
            val1 = this.fromLiteral(val1);
            if (val1 == null) {
                return 0;
            }
        }
        if (val2 != null) {
            val2 = this.fromLiteral(val2);
            if (val2 == null) {
                return 0;
            }
        }
        val1 = val1 != null ? this.serializeToIndex(val1) : val1;
        val2 = val2 != null ? this.serializeToIndex(val2) : val2;
        val1 = exc1 ? (val1 + 1) : val1;
        val2 = exc2 ? (val2 - 1) : val2;
        return this.type.resolveRange(this, val1, val2);
    }

    resolveAll() {
        return this.type.resolveAll(this);
    }

    resolveValue(val) {
        val = this.fromLiteral(val);
        if (val == null) {
            return 0;
        }
        val = this.serializeToIndex(val);
        return this.type.resolveValue(this, val);
    }

    resolveUniqueValue({value, serialize}) {
        if (serialize) {
            value = this.serializeToIndex(value);
        }
        if (this.acceptRange) {
            return this.type.resolveRange(this, value, value);
        }
        return this.type.resolveValue(this, value);
    }

    resolveKeys(...args) {
        return this.type.resolveKeys(this, ...args);
    }

    id2fk(bitmap) {
        return this.type.id2fk(this, bitmap);
    }

    fk2id(bitmap) {
        return this.type.fk2id(this, bitmap);
    }

    increment(v) {
        return this.type.increment(this, v);
    }

    decrement(v) {
        return this.type.decrement(this, v);
    }

    getFieldOptions() {
        return {
            type: String(this.type),
            ...utils.filter(this.options, (k, v) => {
                if (k.startsWith('_')) {
                    return false;
                }
                if (k == 'evaluate' && v == '') {
                    return false;
                }
                return true;
            }),
        };
    }
}

exports.Field = Field;

class FieldError extends Error {}

exports.FieldError = FieldError;

class FieldInvalidTypeError extends FieldError {}

exports.FieldInvalidTypeError = FieldInvalidTypeError;

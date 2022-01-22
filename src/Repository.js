const path = require('path');

const utils = require('ejz-utils');

const {Terms} = require('./Terms');
const {Field} = require('./Field');
const {Bitmap} = require('./Bitmap');
const {QueryParser, QueryParserError} = require('./QueryParser');
const {RecordDiskCache} = require('./RecordDiskCache');

const {
    IDS_DIR,
    FIELDS_DIR,
    IDS_FILE,
    AUTOID_FILE,
    IS_ARR_BEG,
    IS_ARR_REM,
    IS_ARR_ADD_REM,
    TRIM_ARR_ADD_REM,
} = require('./constants');

class Repository {
    constructor(name, fields, options) {
        fields = fields ?? {};
        options = options ?? {};
        utils.ok.string(name);
        utils.ok.object(fields);
        utils.ok.object(options);
        utils.ok.dir(options.root);
        this.name = name;
        let root = options.root;
        delete options.root;
        options.sync_on_delete = options.sync_on_delete ?? true;
        options.sync_on_insert = options.sync_on_insert ?? false;
        this.options = options;
        this.root = (...parts) => path.resolve(root, ...parts);
        utils.mkdir(this.root(IDS_DIR));
        utils.mkdir(this.root(FIELDS_DIR));
        this.ids = new Bitmap({persist: true, file: this.root(IDS_FILE)});
        this.autoid = new Bitmap({persist: true, file: this.root(AUTOID_FILE)});
        let _fields = utils.remap(fields, (k, v) => {
            if (v.type == 'Unique') {
                return;
            }
            v.all = this.ids;
            v.folder = this.root(FIELDS_DIR, k);
            let f = new Field(k, v);
            f.repository = this;
            return [k, f];
        });
        let _unique = utils.remap(fields, (k, v) => {
            if (v.type != 'Unique') {
                return;
            }
            if (Object.keys(v).length != 2) {
                throw new RepositoryInvalidUniqueFieldsError(k);
            }
            return [k, v.fields ?? []];
        });
        let types = {
            All: (f) => (f instanceof Field),
            Unique: (f) => Array.isArray(f),
            Disk: (f) => (f instanceof Field) && (f.isDisk),
            Index: (f) => (f instanceof Field) && (f.isIndex),
            DiskIndex: (f) => (f instanceof Field) && (f.isDiskIndex),
            DiskIndexRequire: (f) => (f instanceof Field) && (f.isDiskIndexRequire),
            Fulltext: (f) => (f instanceof Field) && (f.isFulltext),
            Parent: (f) => (f instanceof Field) && (f.isParent),
            Counter: (f) => (f instanceof Field) && (f.isCounter),
            Array: (f) => (f instanceof Field) && (f.isArray),
            Secondary: (f) => (f instanceof Field) && (f.isSecondary),
            AcceptKeys: (f) => (f instanceof Field) && (f.acceptKeys),
        };
        this.fields = {};
        for (let [key, filter] of Object.entries(types)) {
            this.fields[key] = utils.filter({..._fields, ..._unique}, (k, v) => filter(v));
            this.fields[key + 'Keys'] = Object.keys(this.fields[key]);
            this.fields[key + 'Values'] = Object.values(this.fields[key]);
            this.fields[key + 'Entries'] = Object.entries(this.fields[key]);
            this.fields[key + 'Length'] = this.fields[key + 'Keys'].length;
        }
        this._id = new Field('', {type: 'Id', all: this.ids});
        this.queryParser = new QueryParser();
        this.recordDiskCache = new RecordDiskCache({
            key2file: (key) => this.root(IDS_DIR, String(key)),
            fields: this.fields.Disk,
        });
        utils.each(this.fields.Unique, (k, fields) => {
            let filter = (name) => {
                let f = this.fields.All[name];
                return !f || !f.isSearchable || f.isFulltext;
            };
            if (!fields.length || fields.filter(filter).length) {
                throw new RepositoryInvalidUniqueFieldsError(k);
            }
        });
        utils.each(this.fields.Secondary, (k, field) => {
            if (field.isDisk) {
                throw new RepositoryInvalidSecondaryFieldError(k);
            }
            let filter = (name) => {
                let f = this.fields.All[name];
                return !f || f.isSecondary;
            };
            let {fields} = field.options._evaluate;
            if (!fields.length || fields.filter(filter).length) {
                throw new RepositoryInvalidSecondaryFieldError(k);
            }
        });
        let {AllKeys: ak, CounterKeys: ck, SecondaryKeys: sk} = this.fields;
        this.bare = {
            values: utils.combine(utils.diff(ak, [...ck, ...sk]), null),
            counters: utils.combine(ck, 0),
        };
    }

    toString() {
        return this.name;
    }

    get(id, fields, checkHas, checkFields) {
        checkHas = checkHas ?? true;
        checkFields = checkFields ?? true;
        if (checkHas && !this.has(id)) {
            return null;
        }
        if (checkFields && fields != null) {
            this.checkFields(fields, null, true);
        }
        let {All, AllKeys} = this.fields;
        fields = fields ?? AllKeys;
        let secondary, record, rdc = this.recordDiskCache;
        let values = fields.reduce((acc, name) => {
            let keys = name.split('.');
            name = keys.shift();
            let field = All[name];
            if (field.isSecondary) {
                secondary = secondary ?? [];
                secondary.push(name);
                return acc;
            }
            let value;
            if (field.isDisk) {
                record = record ?? rdc.load(id);
                value = record[name] ?? null;
            } else {
                value = field.get(id);
            }
            if (value != null) {
                if (keys.length) {
                    value = field.resolveKeys(value, ...keys);
                    if (value != null) {
                        let key = name + '.' + keys.join('.');
                        acc[key] = value;
                    }
                } else {
                    acc[name] = value;
                }
            }
            return acc;
        }, {});
        if (secondary) {
            let tmp = Object.assign(utils.combine(fields, null), values);
            let extend = this.extendSecondary(id, tmp, {fields: secondary, extend: true, record});
            values = Object.assign(values, extend);
        }
        return values;
    }

    nextid() {
        let {max} = this.autoid;
        let id = max ? max + 1 : 1;
        this.autoid.add(id);
        if (max) {
            this.autoid.remove(max);
        }
        return id;
    }

    sync() {
        this.ids.sync();
        this.autoid.sync();
        for (let field of this.fields.AllValues) {
            field.sync();
        }
        this.recordDiskCache.sync();
    }

    min() {
        return this.ids.min;
    }

    max() {
        return this.ids.max;
    }

    count() {
        return this.ids.count;
    }

    has(id) {
        return this.ids.has(id);
    }

    insert(values) {
        values = values ?? {};
        values = this.fromExternal(values, false, false);
        let extend = this.extendSecondary(null, values, {});
        values = Object.assign(values, extend);
        this.checkUniqueValues(null, values, false);
        let id = this.nextid();
        this.ids.add(id);
        let record;
        let {All} = this.fields;
        for (let [k, v] of Object.entries(values)) {
            let field = All[k];
            if (field.isDisk) {
                record = record ?? {};
                record[k] = v;
            }
            if (field.isIndex) {
                field.set(id, null, v);
            }
            if (field.isParent) {
                let parent = field.options._parent;
                let rep = this.repositories[parent];
                if (rep) {
                    let counter = this.findCounterField(rep);
                    if (counter != null) {
                        counter.increment(v);
                    }
                }
            }
        }
        if (record) {
            this.recordDiskCache.save(id, record);
            if (this.options.sync_on_insert) {
                this.recordDiskCache.sync(id);
            }
        }
        return id;
    }

    delete(id) {
        if (!this.has(id)) {
            return false;
        }
        let record;
        let {
            DiskLength,
            IndexEntries,
            DiskIndexRequireLength,
            DiskIndexLength,
        } = this.fields;
        let rdc = this.recordDiskCache;
        let dir = DiskIndexRequireLength;
        let di = DiskIndexLength - dir;
        if (dir || (di && rdc.exists(id))) {
            record = rdc.load(id);
        }
        for (let [name, field] of IndexEntries) {
            if (field.isParent) {
                let parent = field.options._parent;
                let rep = this.repositories[parent];
                if (rep) {
                    let counter = this.findCounterField(rep);
                    if (counter != null) {
                        let pid = field.get(id);
                        if (pid != null) {
                            counter.decrement(pid);
                            field.set(id, pid, null);
                        }
                        continue;
                    }
                }
            }
            if (!field.isDisk) {
                field.set(id, undefined, null);
                continue;
            }
            let oldv = record ? (record[name] ?? null) : undefined;
            field.set(id, oldv, null);
        }
        this.ids.remove(id);
        if (DiskLength) {
            rdc.save(id, {});
            if (this.options.sync_on_delete) {
                rdc.sync(id);
            }
        }
        return true;
    }

    update(id, values) {
        if (!this.has(id)) {
            throw new RepositoryIdNotExistsError(id);
        }
        let {All, DiskLength, SecondaryEntries} = this.fields;
        values = this.fromExternal(values, true, true);
        let counters = values[1]; // d, di, dir, d-arr
        values = values[0];
        let _keys = Object.keys(values);
        let extend = this.extendSecondary(id, values, {
            fields: SecondaryEntries.filter(([, v]) => {
                return utils.intersect(_keys, v.options._evaluate.fields).length;
            }).map(([k]) => k),
            extend: true,
            fromUpdate: true,
        });
        values = Object.assign(values, extend);
        let sum = counters[0] + counters[1] + counters[2];
        this.checkUniqueValues(id, values, true);
        let updates, record;
        let rdc = this.recordDiskCache;
        if (
            counters[2] || // dir
            counters[3] || // d-arr
            (sum && sum < DiskLength) ||
            (counters[1] && rdc.exists(id))
        ) {
            record = rdc.load(id);
        }
        for (let [k, v] of Object.entries(values)) {
            let field = All[k];
            if (field.isParent) {
                let parent = field.options._parent;
                let rep = this.repositories[parent];
                if (rep) {
                    let counter = this.findCounterField(rep);
                    if (counter != null) {
                        let pid = field.get(id);
                        if (pid != v) {
                            if (pid != null) {
                                counter.decrement(pid);
                            }
                            if (v != null) {
                                counter.increment(v);
                            }
                        }
                        field.set(id, pid, v);
                        continue;
                    }
                }
            }
            if (field.isIndex && !field.isDisk) {
                field.set(id, undefined, v);
                continue;
            }
            if (!field.isDisk) {
                continue;
            }
            if (field.isArray && v != null && !Array.isArray(v)) {
                let oldv = record[k] ?? null;
                let newv = [...(oldv ?? [])];
                newv.unshift(...(v.addbeg ?? []));
                newv.push(...(v.addend ?? []));
                if (field.isSet) {
                    newv = utils.unique(newv);
                }
                for (let rem of (v.rembeg ?? [])) {
                    let idx = newv.indexOf(rem);
                    if (~idx) {
                        newv.splice(idx, 1);
                    }
                }
                for (let rem of (v.remend ?? [])) {
                    let idx = newv.lastIndexOf(rem);
                    if (~idx) {
                        newv.splice(idx, 1);
                    }
                }
                v = newv;
            }
            let oldv = record ? (record[k] ?? null) : undefined;
            if (utils.equals(oldv, v, {ignoreArrayOrder: field.isSet})) {
                continue;
            }
            updates = updates ?? {};
            updates[k] = v;
            if (record) {
                record[k] = v;
            }
            if (field.isIndex) {
                field.set(id, oldv, v);
            }
        }
        if (updates) {
            rdc.save(id, record || updates);
        }
    }

    extendSecondary(id, values, {fields, extend, record, fromUpdate}) {
        let cache = Object.create(null);
        extend = extend ?? false;
        let {All, SecondaryEntries: entries} = this.fields;
        let checkup = (f) => (values[f] != null) || (extend && !(f in values));
        if (fields != null) {
            entries = entries.filter(([k]) => fields.includes(k));
        }
        let rdc = this.recordDiskCache;
        let ret = {};
        entries.forEach(([k, field]) => {
            let {expression, fields} = field.options._evaluate;
            if (!fields.every(checkup)) {
                if (fromUpdate) {
                    ret[k] = null;
                }
                return;
            }
            let collect = {};
            for (let field of fields) {
                let v = values[field];
                if (v == null) {
                    if (!(field in cache)) {
                        let f = All[field];
                        if (f.isDisk) {
                            record = record ?? rdc.load(id);
                            cache[field] = record[field] ?? null;
                        } else {
                            cache[field] = f.get(id);
                        }
                    }
                    v = cache[field];
                    if (v == null) {
                        if (fromUpdate) {
                            ret[k] = null;
                        }
                        return;
                    }
                }
                collect[field] = v;
            }
            let secondary;
            try {
                secondary = utils.evalwith(expression, collect);
                secondary = secondary != null ? field.fromExternal(secondary) : null;
            } catch (e) {
                throw new RepositoryEvaluateExpressionError([k, String(e)].join(': '));
            }
            if (secondary == null && field.isNotNull) {
                throw new RepositoryValueNullError(k);
            }
            if (secondary != null || fromUpdate) {
                ret[k] = secondary;
            }
        });
        return ret;
    }

    checkUniqueValues(id, values, extend) {
        let cache = Object.create(null);
        let {All, UniqueEntries} = this.fields;
        let checkup = (f) => (values[f] != null) || (extend && !(f in values));
        UniqueEntries.forEach(([k, fields]) => {
            if (!fields.every(checkup)) {
                return;
            }
            let check = {};
            for (let field of fields) {
                let v = values[field];
                let serialize = v != null;
                if (!serialize) {
                    if (!(field in cache)) {
                        cache[field] = All[field].get(id, false);
                    }
                    v = cache[field];
                    if (v == null) {
                        return;
                    }
                }
                check[field] = {value: v, serialize};
            }
            let un = this.checkUniqueValue(check);
            if (un != null && un != id) {
                check = utils.remap(check, (k, {value, serialize}) => serialize ? [k, value] : null);
                throw new RepositoryUniqueError([k, utils.toJson(check)].join(': '));
            }
        });
    }

    checkUniqueValue(values) {
        let {All} = this.fields;
        let bitmap = Bitmap.and(...Object.entries(values).map(([k, v]) => {
            return All[k].resolveUniqueValue(v);
        }));
        let {count} = bitmap;
        if (count > 1) {
            throw new RepositoryUniqueBrokenError(utils.toJson(values));
        }
        return count ? bitmap.toArray()[0] : null;
    }

    iterate(query, fields, sort, asc, miss, random) {
        query = query ?? '*';
        let _query = query;
        if (fields != null) {
            this.checkFields(fields, null);
        }
        let {All, AllKeys} = this.fields;
        fields = fields ?? AllKeys;
        miss = miss ?? 0;
        let getRecords = !!fields.length;
        asc = asc ?? true;
        random = random ?? null;
        if (random) {
            sort = null;
        }
        if (sort != null && !All[sort]) {
            throw new RepositoryInvalidFieldError(sort);
        }
        sort = sort != null ? All[sort] : null;
        if (sort != null && !sort.isSortable) {
            throw new RepositoryInvalidSortableFieldError(sort);
        }
        if (sort != null && !sort.isNotNull) {
            query = `(${query}) @${sort}`;
        }
        let bitmap;
        try {
            let terms = new Terms();
            let tokens = this.queryParser.tokenize(query);
            tokens = this.queryParser.normalize(tokens);
            let infix = this.queryParser.tokens2infix(tokens, terms);
            let postfix = this.queryParser.infix2postfix(infix);
            bitmap = this.resolve(postfix, terms);
        } catch (err) {
            let expected = (err instanceof QueryParserError) || (err instanceof RepositoryError);
            let msg = [err.constructor.name, expected ? err.message : null, _query];
            msg = msg.filter(Boolean).join(': ');
            throw new (expected ? RepositoryQueryError : RepositoryUnknownQueryError)(msg);
        }
        let iterator;
        if (random && bitmap.count) {
            let intervals = this.splitInterval(bitmap.min, bitmap.max, 1000);
            let iterators = intervals.map(([min, max]) => {
                return Bitmap.range(bitmap, min, max).iterator();
            });
            iterator = utils.iteratorSortSync(null, ...iterators);
        } else {
            iterator = sort != null ? sort.sort(bitmap, asc) : bitmap.iterator();
        }
        let repository = this;
        return {
            miss,
            count: bitmap.count,
            done: false,
            offset: 0,
            next() {
                let _value;
                do {
                    let {value, done} = iterator.next();
                    if (done) {
                        this.done = true;
                        return {done: true, value: null};
                    }
                    this.offset++;
                    _value = value;
                } while (miss && (utils.rand(1, 100) <= miss));
                let rec = getRecords ? repository.get(_value, fields, false, false) : {};
                return {done: false, value: [_value, rec]};
            },
            [Symbol.iterator]() {
                return this;
            },
        };
    }

    resolveTerm(terms, i, forceToBitmap) {
        if (i instanceof Bitmap) {
            return i;
        }
        i = +i;
        if (isNaN(i)) {
            throw new RepositoryInternalQueryError();
        }
        let zb = new Bitmap();
        if (forceToBitmap && [0, 1].includes(i)) {
            return i ? this.ids : zb;
        }
        let neg = i < 0;
        let ret = (b) => {
            if (b instanceof Bitmap) {
                return neg ? Bitmap.not(this.ids, b) : b;
            }
            if (![0, 1].includes(b)) {
                throw new RepositoryInternalQueryError();
            }
            b = neg ? (b ? 0 : 1) : b;
            if (forceToBitmap) {
                return b ? this.ids : zb;
            }
            return b;
        };
        i = Math.abs(i);
        let term = terms.get(i);
        let hasValue = term.value != null;
        let hasField = term.field != null;
        let hasInfix = term.infix != null;
        let hasRange = Array.isArray(term.value);
        let isAll = !hasValue && !hasInfix;
        if (hasField) {
            if (hasInfix) {
                let rep = this.repositories[term.field];
                let postfix = this.queryParser.infix2postfix(term.infix);
                let bitmap = rep.resolve(postfix, terms);
                let {parentField} = term;
                if (parentField.repository === this) {
                    bitmap = parentField.fk2id(bitmap);
                } else {
                    bitmap = parentField.id2fk(bitmap);
                }
                return ret(Bitmap.and(this.ids, bitmap));
            }
            let f = this.fields.All[term.field];
            if (isAll) {
                return ret(f.resolveAll());
            }
            if (hasValue && f.acceptRange) {
                let {value} = term;
                value = Array.isArray(value) ? value : [false, value, value, false];
                return ret(f.resolveRange(value));
            }
            if (hasValue) {
                let {value} = term;
                if (f.isFulltext) {
                    value = utils.isObject(value) ? {...value, field: f} : {value, field: f};
                }
                return ret(f.resolveValue(value));
            }
        } else {
            if (hasRange) {
                return ret(this._id.resolveRange(term.value));
            }
            if (hasValue) {
                let values = this.fields.FulltextValues;
                if (!values.length) {
                    return ret(0);
                }
                return ret(Bitmap.or(...values.map((field) => {
                    let {value} = term;
                    value = utils.isObject(value) ? {...value, field} : {value, field};
                    return field.resolveValue(value);
                })));
            }
        }
        throw new RepositoryInternalQueryError();
    }

    resolve(postfix, terms) {
        postfix = postfix.split(' ');
        let len = postfix.length;
        let {All, FulltextLength} = this.fields;
        for (let i = 0; i < len; i++) {
            let term = +postfix[i];
            if (isNaN(term) || [0, 1].includes(term)) {
                continue;
            }
            let neg = term < 0;
            term = Math.abs(term);
            term = terms.get(term);
            let hasValue = term.value != null;
            let hasField = term.field != null;
            let hasInfix = term.infix != null;
            let hasRange = Array.isArray(term.value);
            let isAll = !hasValue && !hasInfix;
            if (hasField) {
                if (hasInfix) {
                    let rep = this.repositories[term.field];
                    if (!rep) {
                        throw new RepositoryInvalidRepositoryError(term.field);
                    }
                    let parentField = this.findParentField(rep);
                    if (!parentField) {
                        return neg ? '0' : '1';
                    }
                    term.parentField = parentField ?? null;
                } else {
                    let f = All[term.field];
                    if (!f) {
                        throw new RepositoryInvalidFieldError(term.field);
                    }
                    if (!f.isSearchable) {
                        throw new RepositoryFieldNotSearchableError(term.field);
                    }
                    if (isAll && !f.acceptAll) {
                        throw new RepositoryFieldRequireValueError(term.field);
                    }
                    if (hasRange && !f.acceptRange) {
                        throw new RepositoryFieldNotRangeableError(term.field);
                    }
                    if (isAll && f.isNotNull) {
                        postfix[i] = neg ? '0' : '1';
                    }
                }
            } else {
                if (hasRange && term.value[1] == null && term.value[2] == null) {
                    postfix[i] = neg ? '0' : '1';
                }
                if (!hasRange && !FulltextLength) {
                    postfix[i] = neg ? '1' : '0';
                }
            }
        }
        postfix = postfix.join(' ').split(' ');
        let stack = [];
        stack.pop = function() {
            let el = Array.prototype.pop.call(this);
            if (el == null) {
                throw new RepositoryInternalQueryError();
            }
            return el;
        };
        for (let part of postfix) {
            let term = +part;
            if (!isNaN(term)) {
                stack.push(term);
                continue;
            }
            let or = part.startsWith('|');
            let and = part.startsWith('&');
            if (and || or) {
                let count = +part.substring(1);
                let collect = [];
                let has_0, has_1;
                while (count--) {
                    let el = stack.pop();
                    if (el == 1) {
                        has_1 = true;
                    } else if (el == 0) {
                        has_0 = true;
                    }
                    collect.push(el);
                }
                if (or && has_1) {
                    stack.push(1);
                    continue;
                }
                if (and && has_0) {
                    stack.push(0);
                    continue;
                }
                collect = utils.unique(collect);
                let checkup = collect.filter((t) => !isNaN(+t));
                let complement = checkup.length != utils.unique(checkup.map((t) => Math.abs(t))).length;
                if (complement) {
                    stack.push(or ? 1 : 0);
                    continue;
                }
                if (or && has_0) {
                    collect = collect.filter((t) => t != 0);
                }
                if (and && has_1) {
                    collect = collect.filter((t) => t != 1);
                }
                if (!collect.length) {
                    stack.push(or ? 0 : 1);
                    continue;
                }
                let args = [];
                for (let item of collect) {
                    let term = this.resolveTerm(terms, item, false);
                    if (isNaN(+term)) {
                        args.push(term);
                        continue;
                    }
                    if ((term == 1 && or) || (term == 0 && and)) {
                        stack.push(term);
                        args = null;
                        break;
                    }
                }
                if (args != null) {
                    if (!args.length) {
                        stack.push(or ? 0 : 1);
                    } else {
                        stack.push(Bitmap[or ? 'or' : 'and'](...args));
                    }
                }
                continue;
            }
            if (part == '-') {
                let term = stack.pop();
                if (!isNaN(+term)) {
                    stack.push([0, 1].includes(term) ? (term ? 0 : 1) : (-term));
                } else {
                    stack.push(Bitmap.not(this.ids, term));
                }
                continue;
            }
            throw new RepositoryInternalQueryError();
        }
        let term = stack.pop();
        if (stack.length) {
            throw new RepositoryInternalQueryError();
        }
        return this.resolveTerm(terms, term, true);
    }

    fromExternal(values, fromUpdate, diskCounters) {
        let arr_add_rem = Object.create(null);
        if (fromUpdate) {
            values = utils.filter(values, (k, v) => v !== undefined);
            let keys = Object.keys(values);
            let split = [[], []];
            keys.forEach((k) => split[+IS_ARR_ADD_REM(k)].push(k));
            this.checkFields(split[0], null);
            split[2] = utils.unique(split[1].map(TRIM_ARR_ADD_REM));
            for (let key of split[2]) {
                if (split[0].includes(key)) {
                    throw new RepositoryKeyCollisionError(key);
                }
            }
            this.checkFields(split[2], this.fields.Array);
            for (let key of split[1]) {
                let value = values[key];
                delete values[key];
                if (value == null) {
                    continue;
                }
                let pri = TRIM_ARR_ADD_REM(key);
                let arr_rem = IS_ARR_REM(key);
                let arr_beg = IS_ARR_BEG(key);
                arr_add_rem[pri] = true;
                values[pri] = values[pri] ?? {};
                let sec = (arr_rem ? 'rem' : 'add') + (arr_beg ? 'beg' : 'end');
                values[pri][sec] = values[pri][sec] ?? [];
                values[pri][sec].push(value);
            }
        } else {
            this.checkFields(Object.keys(values), null);
            values = Object.assign(utils.clone(this.bare.values), values);
        }
        let {All} = this.fields;
        let counters = [0, 0, 0, 0]; // d, di, dir, d-arr
        values = utils.remap(values, (k, v) => {
            let field = All[k];
            if (field.isCounter) {
                throw new RepositoryCounterError(k);
            }
            if (field.isSecondary) {
                throw new RepositorySecondaryError(k);
            }
            let is_arr = arr_add_rem[k];
            if (is_arr) {
                v = utils.remap(v, (k, v) => {
                    v = v.map((v) => field.fromExternal(v)).filter((v) => v != null);
                    v = v.length ? v.flat(1) : null;
                    return v != null ? [k, v] : null;
                });
                if (!Object.keys(v).length) {
                    return;
                }
            } else {
                v = v != null ? field.fromExternal(v) : v;
            }
            if (v == null && field.isNotNull) {
                throw new RepositoryValueNullError(k);
            }
            if (v != null || fromUpdate) {
                if (diskCounters && field.isDisk) {
                    let idx = field.isDiskIndexRequire ? 2 : +field.isDiskIndex;
                    counters[idx]++;
                    if (is_arr) {
                        counters[3]++;
                    }
                }
                return [k, v];
            }
        });
        if (!fromUpdate) {
            values = Object.assign(values, this.bare.counters);
        }
        return diskCounters ? [values, counters] : values;
    }

    checkFields(fields, Dict, acceptKeys) {
        let {All, AcceptKeys} = this.fields;
        Dict = Dict ?? All;
        acceptKeys = acceptKeys ?? false;
        let name = fields.find((name) => {
            if (name.includes('.')) {
                if (!acceptKeys) {
                    return true;
                }
                if (name.endsWith('.') || name.includes('..')) {
                    return true;
                }
                name = name.split('.').shift();
                return !AcceptKeys[name];
            }
            return !Dict[name];
        });
        if (name != null) {
            throw new RepositoryInvalidFieldError(name);
        }
    }

    findParentField(rep) {
        let filter = (n) => (({options: {_parent: p}}) => (p == n));
        let f1 = this.fields.ParentValues.filter(filter(rep.name));
        let f2 = rep.fields.ParentValues.filter(filter(this.name));
        let len = f1.length + f2.length;
        if (len > 1) {
            let msg = [String(this), String(rep)].join(' <-> ');
            throw new RepositoryParentKeyError(msg);
        }
        return f1[0] ?? f2[0] ?? null;
    }

    findCounterField(rep) {
        let filter = ({options: {_child: c}}) => (c == this.name);
        let fields = rep.fields.CounterValues.filter(filter);
        if (fields.length > 1) {
            let msg = [String(this), String(rep)].join(' <-> ');
            throw new RepositoryCounterKeyError(msg);
        }
        return fields[0] ?? null;
    }

    getFieldOptions() {
        return utils.map({...this.fields.All, ...this.fields.Unique}, (f) => {
            if (f.getFieldOptions) {
                return f.getFieldOptions();
            }
            return {type: 'Unique', fields: f};
        });
    }

    splitInterval(min, max, n) {
        let c = max - min + 1;
        n = Math.min(n, c);
        let s = c / n;
        let ret = [];
        let ex;
        for (let i = min; i <= max; i += s) {
            let a = Math.floor(i);
            let b = Math.min(max, Math.floor(i + s) - 1);
            if (ex != null && a == ex) {
                a++;
            }
            ret.push([a, b]);
            ex = b;
        }
        return ret;
    }
}

exports.Repository = Repository;

class RepositoryError extends Error {}

exports.RepositoryError = RepositoryError;

class RepositoryInvalidFieldError extends RepositoryError {}

exports.RepositoryInvalidFieldError = RepositoryInvalidFieldError;

class RepositoryIdNotExistsError extends RepositoryError {}

exports.RepositoryIdNotExistsError = RepositoryIdNotExistsError;

class RepositoryInvalidSortableFieldError extends RepositoryError {}

exports.RepositoryInvalidSortableFieldError = RepositoryInvalidSortableFieldError;

class RepositoryFieldNotSearchableError extends RepositoryError {}

exports.RepositoryFieldNotSearchableError = RepositoryFieldNotSearchableError;

class RepositoryFieldNotRangeableError extends RepositoryError {}

exports.RepositoryFieldNotRangeableError = RepositoryFieldNotRangeableError;

class RepositoryInternalQueryError extends RepositoryError {}

exports.RepositoryInternalQueryError = RepositoryInternalQueryError;

class RepositoryFieldRequireValueError extends RepositoryError {}

exports.RepositoryFieldRequireValueError = RepositoryFieldRequireValueError;

class RepositoryValueNullError extends RepositoryError {}

exports.RepositoryValueNullError = RepositoryValueNullError;

class RepositoryInvalidUniqueFieldsError extends RepositoryError {}

exports.RepositoryInvalidUniqueFieldsError = RepositoryInvalidUniqueFieldsError;

class RepositoryUniqueError extends RepositoryError {}

exports.RepositoryUniqueError = RepositoryUniqueError;

class RepositoryUniqueBrokenError extends RepositoryError {}

exports.RepositoryUniqueBrokenError = RepositoryUniqueBrokenError;

class RepositoryInvalidRepositoryError extends RepositoryError {}

exports.RepositoryInvalidRepositoryError = RepositoryInvalidRepositoryError;

class RepositoryParentKeyError extends RepositoryError {}

exports.RepositoryParentKeyError = RepositoryParentKeyError;

class RepositoryCounterError extends RepositoryError {}

exports.RepositoryCounterError = RepositoryCounterError;

class RepositoryCounterKeyError extends RepositoryError {}

exports.RepositoryCounterKeyError = RepositoryCounterKeyError;

class RepositoryKeyCollisionError extends RepositoryError {}

exports.RepositoryKeyCollisionError = RepositoryKeyCollisionError;

class RepositorySecondaryError extends RepositoryError {}

exports.RepositorySecondaryError = RepositorySecondaryError;

class RepositoryInvalidSecondaryFieldError extends RepositoryError {}

exports.RepositoryInvalidSecondaryFieldError = RepositoryInvalidSecondaryFieldError;

class RepositoryEvaluateExpressionError extends RepositoryError {}

exports.RepositoryEvaluateExpressionError = RepositoryEvaluateExpressionError;

class RepositoryQueryError extends RepositoryError {}

exports.RepositoryQueryError = RepositoryQueryError;

class RepositoryUnknownQueryError extends RepositoryError {}

exports.RepositoryUnknownQueryError = RepositoryUnknownQueryError;

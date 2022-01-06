const path = require('path');

const utils = require('ejz-utils');

const {Bitmap} = require('./Bitmap');
const {BITMAP_ALL_FILE, SPLIT_ARR_ADD_REM} = require('./constants');

class MultipleBitmaps {
    constructor(options) {
        options = options ?? {};
        this.options = options;
        this.bitmaps = Object.create(null);
        if (this.options.folder != null) {
            let files = utils.listDirectory(this.options.folder);
            let keys = files.map((f) => path.basename(f));
            keys.forEach((k) => this.createBitmap(k));
        }
        this.options.all = this.options.all ?? false;
        this.externalAll = false;
        this.all = false;
        if (this.options.all instanceof Bitmap) {
            this.externalAll = true;
            this.all = this.options.all;
        } else if (this.options.all === true) {
            let file = null;
            if (this.options.folder != null) {
                file = path.resolve(this.options.folder, BITMAP_ALL_FILE);
            }
            this.all = new Bitmap({persist: true, file});
        } else if (this.options.all !== false) {
            throw new MultipleBitmapsInvalidOptionAllError();
        }
        delete this.options.all;
    }

    createBitmap(key) {
        let file = null;
        if (this.options.folder != null) {
            file = path.resolve(this.options.folder, String(key));
        }
        let bitmap = new Bitmap({persist: true, file});
        this.bitmaps[key] = bitmap;
        return bitmap;
    }

    has(bit) {
        let {all} = this;
        if (!all) {
            throw new MultipleBitmapsInvalidAllError(bit);
        }
        return all.has(bit);
    }

    add(bit, values) {
        if (values == null) {
            throw new MultipleBitmapsValueIsNullError(bit);
        }
        let {bitmaps, all, externalAll} = this;
        if (all && !externalAll) {
            if (!all.tryAdd(bit)) {
                throw new MultipleBitmapsAlreadyAddedError(bit);
            }
        }
        for (let value of values) {
            let bitmap = bitmaps[value] ?? this.createBitmap(value);
            bitmap.add(bit);
        }
    }

    remove(bit, values) {
        let {bitmaps, all, externalAll} = this;
        if (all && !externalAll) {
            if (!all.tryRemove(bit)) {
                return false;
            }
        }
        if (values != null) {
            for (let value of values) {
                let bitmap = bitmaps[value];
                if (bitmap) {
                    bitmap.remove(bit);
                }
            }
        } else {
            for (let key in bitmaps) {
                bitmaps[key].remove(bit);
            }
        }
        return true;
    }

    update(bit, val1, val2) {
        if (val2 == null) {
            throw new MultipleBitmapsValueIsNullError(bit);
        }
        let {bitmaps} = this;
        if (!Array.isArray(val2)) {
            if (val1 != null) {
                throw new MultipleBitmapsInvalidArgumentsError(bit);
            }
            [val1, val2] = SPLIT_ARR_ADD_REM(val2);
            val1 = utils.unique(val1);
            val2 = utils.unique(val2);
        }
        let _val2 = utils.combine(val2, true);
        if (val1 != null) {
            for (let v of val1.filter((v) => !_val2[v])) {
                let bitmap = bitmaps[v];
                if (bitmap) {
                    bitmap.remove(bit);
                }
            }
        } else {
            for (let key in bitmaps) {
                if (!_val2[key]) {
                    bitmaps[key].remove(bit);
                }
            }
        }
        let _val1 = val1 != null ? utils.combine(val1, true) : null;
        let add = _val1 != null ? val2.filter((v) => !_val1[v]) : val2;
        for (let v of add) {
            let bitmap = bitmaps[v] ?? this.createBitmap(v);
            bitmap.add(bit);
        }
    }

    sync(key) {
        if (key != null) {
            let bitmap = this.bitmaps[key];
            if (bitmap) {
                bitmap.sync();
            }
            return;
        }
        for (let key in this.bitmaps) {
            this.bitmaps[key].sync();
        }
        if (this.all && !this.externalAll) {
            this.all.sync();
        }
    }

    getBitmap(value) {
        let bitmap = this.bitmaps[value];
        return bitmap ?? (new Bitmap());
    }

    getBitValue(bit) {
        if (this.all && !this.has(bit)) {
            return null;
        }
        let ret = [];
        for (let key in this.bitmaps) {
            if (this.bitmaps[key].has(bit)) {
                ret.push(key);
            }
        }
        return ret;
    }
}

exports.MultipleBitmaps = MultipleBitmaps;

class MultipleBitmapsError extends Error {}

exports.MultipleBitmapsError = MultipleBitmapsError;

class MultipleBitmapsValueIsNullError extends MultipleBitmapsError {}

exports.MultipleBitmapsValueIsNullError = MultipleBitmapsValueIsNullError;

class MultipleBitmapsInvalidOptionAllError extends MultipleBitmapsError {}

exports.MultipleBitmapsInvalidOptionAllError = MultipleBitmapsInvalidOptionAllError;

class MultipleBitmapsAlreadyAddedError extends MultipleBitmapsError {}

exports.MultipleBitmapsAlreadyAddedError = MultipleBitmapsAlreadyAddedError;

class MultipleBitmapsInvalidAllError extends MultipleBitmapsError {}

exports.MultipleBitmapsInvalidAllError = MultipleBitmapsInvalidAllError;

class MultipleBitmapsInvalidArgumentsError extends MultipleBitmapsError {}

exports.MultipleBitmapsInvalidArgumentsError = MultipleBitmapsInvalidArgumentsError;

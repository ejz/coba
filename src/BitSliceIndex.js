const path = require('path');

const utils = require('ejz-utils');

const {MAX, BSI_ALL_FILE} = require('./constants');
const {Bitmap} = require('./Bitmap');

class BitSliceIndex {
    constructor(options) {
        options = options ?? {};
        this.max = options.max ?? MAX;
        this.folder = options.folder ?? null;
        this.max = Math.ceil(this.max);
        this.max = Math.max(0, this.max);
        this.rank = this.max.toString(2).length;
        this.bitmaps = {
            zero: new Array(this.rank).fill().map((_, i) => {
                let file = null;
                if (this.folder != null) {
                    file = path.resolve(this.folder, String(i));
                }
                return new Bitmap({persist: true, file});
            }),
        };
        this.externalAll = false;
        if (options.all != null) {
            utils.ok.instance(Bitmap, options.all);
            this.externalAll = true;
            this.bitmaps.all = options.all;
        } else {
            let file = null;
            if (this.folder != null) {
                file = path.resolve(this.folder, BSI_ALL_FILE);
            }
            this.bitmaps.all = new Bitmap({persist: true, file});
        }
    }

    splitValueToBinary(value) {
        return value.toString(2).padStart(this.rank, '0').split('');
    }

    sync() {
        let {bitmaps: {all, zero}} = this;
        if (!this.externalAll) {
            all.sync();
        }
        zero.forEach((b) => b.sync());
    }

    clear() {
        let {bitmaps: {all, zero}} = this;
        if (!this.externalAll) {
            all.clear();
        }
        zero.forEach((bitmap) => bitmap.clear());
    }

    has(bit) {
        return this.bitmaps.all.has(bit);
    }

    add(bit, value) {
        if (value == null) {
            throw new BitSliceIndexValueIsNullError(bit);
        }
        let {bitmaps: {all, zero}} = this;
        if (!this.externalAll) {
            if (!all.tryAdd(bit)) {
                throw new BitSliceIndexAlreadyAddedError(bit);
            }
        }
        value = this.splitValueToBinary(value);
        value.forEach((c, i) => {
            if (c == '0') {
                zero[i].add(bit);
            }
        });
    }

    remove(bit, value) {
        let {bitmaps: {all, zero}} = this;
        if (!this.externalAll) {
            if (!all.tryRemove(bit)) {
                return false;
            }
        }
        value = this.splitValueToBinary(value ?? 0);
        value.forEach((c, i) => {
            if (c == '0') {
                zero[i].remove(bit);
            }
        });
        return true;
    }

    update(bit, val1, val2) {
        if (val2 == null) {
            throw new BitSliceIndexValueIsNullError(bit);
        }
        let {bitmaps: {zero}} = this;
        val1 = val1 != null ? this.splitValueToBinary(val1) : val1;
        val2 = this.splitValueToBinary(val2);
        let len = val2.length;
        for (let i = 0; i < len; i++) {
            if (val1 == null || val1[i] != val2[i]) {
                if (val2[i] == '0') {
                    zero[i].add(bit);
                } else {
                    zero[i].remove(bit);
                }
            }
        }
    }

    getBitmap(from, to) {
        let {max} = this;
        from = from ?? 0;
        to = to ?? max;
        if (to < from || to < 0 || max < from) {
            return new Bitmap();
        }
        from = from < 0 ? 0 : from;
        to = max < to ? max : to;
        if (from == to) {
            return this.getBitmapValue(this.splitValueToBinary(from));
        }
        let bitmapTo = this.getBitmapTo(this.splitValueToBinary(to));
        if (!from) {
            return bitmapTo;
        }
        return Bitmap.not(
            bitmapTo,
            this.getBitmapTo(this.splitValueToBinary(from - 1)),
        );
    }

    getBitmapValue(value) {
        let {rank, bitmaps: {all, zero}} = this;
        let t = value.shift();
        let l = value.length;
        let idx = rank - l - 1;
        if (t == '1') {
            let one = Bitmap.not(all, zero[idx]);
            return l ? Bitmap.and(one, this.getBitmapValue(value)) : one;
        }
        return l ? Bitmap.and(zero[idx], this.getBitmapValue(value)) : zero[idx];
    }

    getBitmapTo(to) {
        let {rank, bitmaps: {all, zero}} = this;
        let t = to.shift();
        let l = to.length;
        let idx = rank - l - 1;
        if (t == '1') {
            return l ? Bitmap.or(zero[idx], this.getBitmapTo(to)) : all;
        }
        return l ? Bitmap.and(zero[idx], this.getBitmapTo(to)) : zero[idx];
    }

    *sort(bitmap, asc, l = 0) {
        let {rank, bitmaps: {all, zero}} = this;
        let last = l == rank - 1;
        for (let i of (asc ? [0, 1] : [1, 0])) {
            let and = i ? Bitmap.not(all, zero[l]) : zero[l];
            let intersection = Bitmap.orAndNot('and', 1, bitmap, and);
            let count = intersection.count;
            if (last || count == 1) {
                yield* intersection.iterator();
            } else if (count >= 2) {
                yield* this.sort(intersection, asc, l + 1);
            }
        }
    }

    getBitValue(bit) {
        if (!this.has(bit)) {
            return null;
        }
        let {rank, bitmaps: {zero}} = this;
        let vals = new Array(rank).fill().map((_, i) => (zero[i].has(bit) ? '0' : '1'));
        return parseInt(vals.join(''), 2);
    }

    getBitValues(bits) {
        let values = [];
        bits.forEach((bit) => {
            let val = this.getBitValue(bit);
            if (val != null) {
                values.push(val);
            }
        });
        return values;
    }

    remapBitmap(bitmap, revert) {
        utils.ok.instance(Bitmap, bitmap);
        let bm = new Bitmap();
        for (let bit of bitmap.iterator()) {
            if (revert) {
                bm.or(this.getBitmapValue(this.splitValueToBinary(bit)));
            } else {
                let val = this.getBitValue(bit);
                if (val != null) {
                    bm.add(val);
                }
            }
        }
        return bm;
    }

    increment(bit, inc) {
        let oldv = this.getBitValue(bit);
        if (oldv == null) {
            return;
        }
        inc = inc ?? 1;
        let newv = oldv + inc;
        if (newv < 0 || this.max < newv) {
            throw new BitSliceIndexOutOfRangeError();
        }
        this.update(bit, oldv, newv);
    }
}

exports.BitSliceIndex = BitSliceIndex;

class BitSliceIndexError extends Error {}

exports.BitSliceIndexError = BitSliceIndexError;

class BitSliceIndexOutOfRangeError extends BitSliceIndexError {}

exports.BitSliceIndexOutOfRangeError = BitSliceIndexOutOfRangeError;

class BitSliceIndexAlreadyAddedError extends BitSliceIndexError {}

exports.BitSliceIndexAlreadyAddedError = BitSliceIndexAlreadyAddedError;

class BitSliceIndexValueIsNullError extends BitSliceIndexError {}

exports.BitSliceIndexValueIsNullError = BitSliceIndexValueIsNullError;

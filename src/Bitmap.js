const RoaringBitmap = require('roaring/RoaringBitmap32');
const utils = require('ejz-utils');

const {BitmapDiskCache} = require('./BitmapDiskCache');

const bitmapDiskCache = new BitmapDiskCache({key2file: (key) => key});

exports.bitmapDiskCache = bitmapDiskCache;

class Bitmap {
    constructor(options) {
        options = options ?? {};
        // persist -> file
        this.file = options.file ?? null;
        this.persist = this.file != null || !!options.persist;
        if (this.file != null) {
            if (!utils.isFile(this.file)) {
                this.save(new RoaringBitmap());
                this.sync();
            }
        } else {
            this._roaringBitmap = new RoaringBitmap(options.roaringBitmap ?? []);
        }
    }

    get roaringBitmap() {
        return this._roaringBitmap ?? bitmapDiskCache.load(this.file);
    }

    sync() {
        if (this.file != null) {
            bitmapDiskCache.sync(this.file);
        }
    }

    save(roaringBitmap) {
        if (this.file != null) {
            bitmapDiskCache.save(this.file, roaringBitmap);
        }
    }

    static range(bitmap, min, max) {
        let bmin = bitmap.min;
        let bmax = bitmap.max;
        min = min ?? bmin;
        max = max ?? bmax;
        if (max < min || max < bmin || bmax < min) {
            return new Bitmap();
        }
        bitmap = bitmap.persist ? bitmap.copy() : bitmap;
        bitmap.range(min, max);
        return bitmap;
    }

    static or(...bitmaps) {
        return this.orAndNot('or', null, ...bitmaps);
    }

    static and(...bitmaps) {
        return this.orAndNot('and', null, ...bitmaps);
    }

    static not(not, ...bitmaps) {
        return this.orAndNot('not', 0, not, ...bitmaps);
    }

    static orAndNot(call, index, ...bitmaps) {
        if (!bitmaps.length) {
            return new Bitmap();
        }
        if (bitmaps.length == 1) {
            return bitmaps[0];
        }
        if (index == null) {
            index = bitmaps.findIndex((b) => !b.persist);
            index = ~index ? index : 0;
        }
        let bitmap = bitmaps[index];
        bitmaps.splice(index, 1);
        bitmap = bitmap.persist ? bitmap.copy() : bitmap;
        bitmap[call](...bitmaps);
        return bitmap;
    }

    or(...bitmaps) {
        let {roaringBitmap} = this;
        for (let bitmap of bitmaps) {
            roaringBitmap.orInPlace(bitmap.roaringBitmap);
        }
        this.save(roaringBitmap);
    }

    and(...bitmaps) {
        let {roaringBitmap} = this;
        for (let bitmap of bitmaps) {
            roaringBitmap.andInPlace(bitmap.roaringBitmap);
        }
        this.save(roaringBitmap);
    }

    not(...bitmaps) {
        let {roaringBitmap} = this;
        for (let bitmap of bitmaps) {
            roaringBitmap.andNotInPlace(bitmap.roaringBitmap);
        }
        this.save(roaringBitmap);
    }

    range(min, max) {
        let bmin = this.min;
        let bmax = this.max;
        min = min ?? bmin;
        max = max ?? bmax;
        let {roaringBitmap} = this;
        if (max < min || max < bmin || bmax < min) {
            this.clear();
            return;
        }
        if (bmin < min) {
            roaringBitmap.removeRange(bmin, min);
        }
        if (max < bmax) {
            roaringBitmap.removeRange(max + 1, bmax + 1);
        }
        this.save(roaringBitmap);
    }

    clear() {
        let {roaringBitmap} = this;
        roaringBitmap.clear();
        this.save(roaringBitmap);
    }

    add(...bits) {
        let {roaringBitmap} = this;
        roaringBitmap.addMany(bits);
        this.save(roaringBitmap);
    }

    tryAdd(...bits) {
        let i = 0;
        let {roaringBitmap} = this;
        for (let bit of bits) {
            if (roaringBitmap.tryAdd(bit)) {
                i++;
            }
        }
        if (i) {
            this.save(roaringBitmap);
        }
        return i;
    }

    remove(...bits) {
        let {roaringBitmap} = this;
        roaringBitmap.removeMany(bits);
        this.save(roaringBitmap);
    }

    tryRemove(...bits) {
        let i = 0;
        let {roaringBitmap} = this;
        for (let bit of bits) {
            if (roaringBitmap.delete(bit)) {
                i++;
            }
        }
        if (i) {
            this.save(roaringBitmap);
        }
        return i;
    }

    has(bit) {
        return this.roaringBitmap.has(bit);
    }

    copy() {
        let {roaringBitmap} = this;
        return new Bitmap({roaringBitmap});
    }

    iterator() {
        return this.roaringBitmap.iterator();
    }

    toArray() {
        return this.roaringBitmap.toArray();
    }

    get count() {
        return this.roaringBitmap.size;
    }

    get min() {
        let {roaringBitmap} = this;
        return roaringBitmap.size ? roaringBitmap.minimum() : 0;
    }

    get max() {
        let {roaringBitmap} = this;
        return roaringBitmap.size ? roaringBitmap.maximum() : 0;
    }
}

exports.Bitmap = Bitmap;

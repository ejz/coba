const RoaringBitmap = require('roaring/RoaringBitmap32');

const {DiskCache} = require('./DiskCache');

class BitmapDiskCache extends DiskCache {
    constructor(options, logger) {
        options = options ?? {};
        options.ms = options.ms ?? 3600E3;
        super(options, logger);
    }

    content2buffer(roaringBitmap) {
        return roaringBitmap.serialize(false);
    }

    buffer2content(buffer) {
        return RoaringBitmap.deserialize(buffer, false);
    }
}

exports.BitmapDiskCache = BitmapDiskCache;

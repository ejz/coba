const path = require('path');

const utils = require('ejz-utils');
const RoaringBitmap = require('roaring/RoaringBitmap32');

const {BitmapDiskCache} = require('./BitmapDiskCache');

test('BitmapDiskCache / get, set', async () => {
    let tmp = utils.tempDirectory();
    let file = path.resolve(tmp, String(utils.rand()));
    let bdc = new BitmapDiskCache();
    bdc.save(file, new RoaringBitmap([1, 5, 100]));
    bdc.sync();
    bdc = new BitmapDiskCache();
    expect([...bdc.load(file)]).toEqual([1, 5, 100]);
    bdc.sync();
    let bm = bdc.load(file);
    bm.add(4);
    bdc.save(file, bm);
    bdc.sync();
    bdc = new BitmapDiskCache();
    expect([...bdc.load(file)]).toEqual([1, 4, 5, 100]);
    bdc.sync();
});

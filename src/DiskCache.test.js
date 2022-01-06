const path = require('path');

const utils = require('ejz-utils');

const {DiskCache} = require('./DiskCache');

test('DiskCache / load', async () => {
    let dc = new DiskCache({ms: 100});
    dc.buffer2content = String;
    let tmp = utils.tempDirectory();
    let file = path.resolve(tmp, String(utils.rand()));
    utils.writeFile(file, '1');
    expect(dc.load(file)).toEqual('1');
    expect(dc.load(file)).toEqual('1');
    await utils.sleep(200);
    expect(dc.load(file)).toEqual('1');
    expect(dc.load(file)).toEqual('1');
    await utils.sleep(200);
    dc.save(file, '2');
    expect(dc.load(file)).toEqual('2');
    await utils.sleep(200);
    expect(dc.load(file)).toEqual('2');
    dc.save(file);
    expect(dc.load(file)).toEqual(null);
    dc.sync();
});

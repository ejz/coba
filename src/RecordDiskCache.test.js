const path = require('path');

const utils = require('ejz-utils');

const {Field} = require('./Field');
const {RecordDiskCache} = require('./RecordDiskCache');

test('RecordDiskCache / general', async () => {
    let tmp = utils.tempDirectory();
    let key2file = (id) => path.resolve(tmp, String(id));
    let fields = {
        f1: new Field('f1', {type: 'String'}),
        f2: new Field('f2', {type: 'String'}),
    };
    let rdc = new RecordDiskCache({ms: 50, fields, key2file});
    expect(rdc.load(1)).toEqual({});
    rdc.save(1, {...rdc.load(1), f1: 'foo'});
    expect(rdc.load(1)).toEqual({f1: 'foo'});
    rdc.save(1, {});
    expect(rdc.load(1)).toEqual({});
    await utils.sleep(100);
    expect(rdc.load(1)).toEqual({});
    rdc.save(1, {...rdc.load(1), f1: 'foo'});
    rdc.save(1, {...rdc.load(1), f2: 'bar'});
    expect(rdc.load(1)).toEqual({f1: 'foo', f2: 'bar'});
    rdc.save(1, {...rdc.load(1), f2: 'baz'});
    expect(rdc.load(1)).toEqual({f1: 'foo', f2: 'baz'});
    rdc.sync();
});

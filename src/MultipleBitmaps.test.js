const utils = require('ejz-utils');

const {MultipleBitmaps} = require('./MultipleBitmaps');

test('MultipleBitmaps / common', async () => {
    let mb = new MultipleBitmaps();
    expect(!!mb).toEqual(true);
});

test('MultipleBitmaps / update bug', async () => {
    let mb = new MultipleBitmaps();
    mb.add(1, ['foo', 'bar']);
    expect(mb.getBitValue(1).sort()).toEqual(['foo', 'bar'].sort());
    mb.update(1, utils.rand([null, ['foo', 'bar']]), ['bar', 'baz']);
    expect(mb.getBitValue(1).sort()).toEqual(['baz', 'bar'].sort());
});

test('MultipleBitmaps / arr add rem', async () => {
    let mb = new MultipleBitmaps();
    mb.add(1, ['foo', 'bar']);
    expect(mb.getBitValue(1).sort()).toEqual(['foo', 'bar'].sort());
    mb.update(1, null, {addbeg: ['baz', 'moo'], rembeg: ['foo'], remend: ['bar']});
    expect(mb.getBitValue(1).sort()).toEqual(['baz', 'moo'].sort());
});

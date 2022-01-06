const utils = require('ejz-utils');

const {Field} = require('./Field');
const types = require('./types');

test('Field / types', () => {
    for (let type of Object.values(types)) {
        if (!('keepOnDisk' in type)) {
            continue;
        }
        let f = new Field('field', {type: String(type)});
        expect(f instanceof Field).toEqual(true);
    }
});

test('Field / NumberIndex', () => {
    let tmp = utils.tempDirectory();
    let f = new Field('field', {
        type: 'NumberIndex',
        min: 2,
        max: 10,
        precision: 1,
        folder: tmp,
    });
    let cases = [
        [2.1, 1],
        [2.1111, 1],
        [2.19, 1],
        [10.01, 80],
    ];
    for (let [inp, out] of cases) {
        expect(f.serializeToIndex(inp)).toEqual(out);
    }
    f.set(1, null, 10);
    expect([...f.resolveValue(10).iterator()]).toEqual([1]);
    f.sync();
    f = new Field('field', {
        type: 'NumberIndex',
        min: 2,
        max: 10,
        precision: 1,
        folder: tmp,
    });
    expect([...f.resolveValue(10).iterator()]).toEqual([1]);
    f.sync();
});

test('Field / Fulltext', () => {
    let tmp = utils.tempDirectory();
    let f = new Field('field', {
        type: 'Fulltext',
        folder: tmp,
    });
    f.set(1, null, 'foo bar baz');
    expect([...f.resolveValue({value: 'foo', field: f}).iterator()]).toEqual([1]);
    expect([...f.resolveValue({value: 'foo moo', field: f}).iterator()]).toEqual([]);
    expect([...f.resolveValue({value: '"foo moo"', field: f}).iterator()]).toEqual([]);
    expect([...f.resolveValue({value: 'bar', field: f}).iterator()]).toEqual([1]);
    expect([...f.resolveValue({value: 'baz', field: f}).iterator()]).toEqual([1]);
    f.sync();
    f = new Field('field', {
        type: 'Fulltext',
        folder: tmp,
    });
    expect([...f.resolveValue({value: 'foo', field: f}).iterator()]).toEqual([1]);
    expect([...f.resolveValue({value: 'bar', field: f}).iterator()]).toEqual([1]);
    expect([...f.resolveValue({value: 'baz', field: f}).iterator()]).toEqual([1]);
    f.sync();
});

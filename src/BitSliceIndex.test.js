const utils = require('ejz-utils');

const {BitSliceIndex} = require('./BitSliceIndex');

function get(values, from, to, asc) {
    values = values.filter(([, v]) => from <= v && v <= to);
    if (asc != null) {
        values.sort(
            ([i1, v1], [i2, v2]) => (asc ? v1 - v2 : v2 - v1) || i1 - i2,
        );
    }
    return values.map(([i]) => i);
}

test('BitSliceIndex / update with true', async () => {
    let bsi = new BitSliceIndex();
    bsi.add(2, 7);
    bsi.update(2, true, 9);
    expect(bsi.getBitValue(2)).toEqual(9);
});

test('BitSliceIndex / sort', async () => {
    let bsi = new BitSliceIndex({max: 100});
    let values = [];
    for (let i = 1; i <= 1000; i++) {
        let value = utils.rand(0, 100);
        bsi.add(i, value);
        values.push([i, value]);
    }
    for (let i = 0; i <= 200; i++) {
        let bitmap1 = bsi.getBitmap(i, i);
        expect(get(values, i, i)).toEqual(bitmap1.toArray());
        let bitmap2 = bsi.getBitmap(1, i);
        expect(get(values, 1, i)).toEqual(bitmap2.toArray());
        let rnd = utils.rand(10, 50);
        let bitmap3 = bsi.getBitmap(rnd, i);
        expect(get(values, rnd, i)).toEqual(bitmap3.toArray());
        expect(get(values, 1, i, true)).toEqual([...bsi.sort(bitmap2, true)]);
        expect(get(values, 1, i, false)).toEqual([...bsi.sort(bitmap2, false)]);
        expect(get(values, rnd, i, true)).toEqual([...bsi.sort(bitmap3, true)]);
        expect(get(values, rnd, i, false)).toEqual([
            ...bsi.sort(bitmap3, false),
        ]);
    }
});

test('BitSliceIndex / folder', async () => {
    let folder = utils.tempDirectory();
    let bsi = new BitSliceIndex({folder});
    bsi.add(1, 3);
    bsi.add(100, 300);
    bsi.sync();
    bsi = new BitSliceIndex({folder});
    expect(bsi.getBitmap().toArray()).toEqual([1, 100]);
    bsi.sync();
});

test('BitSliceIndex / getBitValue / 1', async () => {
    let bsi = new BitSliceIndex({
        max: 100000,
    });
    bsi.add(1, 356);
    bsi.add(100, 30000);
    bsi.add(200, 100000);
    expect(bsi.getBitValue(1)).toEqual(356);
    expect(bsi.getBitValue(100)).toEqual(30000);
    expect(bsi.getBitValue(200)).toEqual(100000);
    expect(bsi.getBitValue(300)).toEqual(null);
});

test('BitSliceIndex / remove / 1', async () => {
    let bsi = new BitSliceIndex();
    bsi.add(1, 9);
    expect(bsi.has(1)).toEqual(true);
    expect(bsi.getBitValue(1)).toEqual(9);
    bsi.remove(1);
    expect(bsi.has(1)).toEqual(false);
    expect(bsi.getBitValue(1)).toEqual(null);
    bsi.add(1, 10);
    expect(bsi.has(1)).toEqual(true);
    expect(bsi.getBitValue(1)).toEqual(10);
    bsi.remove(1, 10);
    expect(bsi.has(1)).toEqual(false);
    expect(bsi.getBitValue(1)).toEqual(null);
});

test('BitSliceIndex / getBitValues', async () => {
    let bsi = new BitSliceIndex({max: 10});
    for (let i = 1; i <= 1E3; i++) {
        bsi.add(i, utils.rand(0, 10));
    }
    bsi.getBitValues(new Array(1E3).fill().map((_, i) => i + 1)).forEach(
        (value) => {
            expect(0 <= value && value <= 10).toEqual(true);
        },
    );
});

test('BitSliceIndex / increment + decrement', async () => {
    const bsi = new BitSliceIndex({max: 10});
    bsi.add(2, 8);
    expect(bsi.getBitValue(2)).toEqual(8);
    bsi.increment(2);
    expect(bsi.getBitValue(2)).toEqual(9);
    bsi.increment(2);
    expect(bsi.getBitValue(2)).toEqual(10);
    expect(() => bsi.increment(2)).toThrow();
    bsi.increment(2, -1);
    expect(bsi.getBitValue(2)).toEqual(9);
    bsi.increment(2);
    expect(bsi.getBitValue(2)).toEqual(10);
});

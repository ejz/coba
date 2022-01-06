const utils = require('ejz-utils');

const {Bitmap} = require('./Bitmap');

function newBitmap(...bits) {
    let bitmap = new Bitmap();
    bitmap.add(...bits);
    return bitmap;
}

test('Bitmap / static', async () => {
    expect(Bitmap.or(newBitmap(1, 3), newBitmap(3, 4, 7)).toArray()).toEqual([
        1, 3, 4, 7,
    ]);
    expect(
        Bitmap.and(
            newBitmap(1, 2, 3, 4),
            newBitmap(2, 3, 4, 5),
            newBitmap(3, 4, 5, 6),
        ).toArray(),
    ).toEqual([3, 4]);
    expect(
        Bitmap.not(
            newBitmap(1, 2, 3, 4),
            newBitmap(2, 3),
            newBitmap(1, 7),
        ).toArray(),
    ).toEqual([4]);
    expect(
        Bitmap.range(newBitmap(1, 2, 3, 4, 5, 6, 7), 2, 6).toArray(),
    ).toEqual([2, 3, 4, 5, 6]);
});

test('Bitmap / or, and, not, range', async () => {
    let bm1 = newBitmap(1, 4);
    bm1.or(newBitmap(5));
    expect(bm1.toArray()).toEqual([1, 4, 5]);
    let bm2 = newBitmap(1, 4, 7);
    bm2.and(newBitmap(5, 7));
    expect(bm2.toArray()).toEqual([7]);
    let bm3 = newBitmap(1, 4, 7);
    bm3.not(newBitmap(7, 10));
    expect(bm3.toArray()).toEqual([1, 4]);
    let bm4 = newBitmap(1, 2, 3, 4, 5, 6, 7);
    bm4.range(2, 6);
    expect(bm4.toArray()).toEqual([2, 3, 4, 5, 6]);
});

test('Bitmap / add, remove, clear', async () => {
    let bm = new Bitmap();
    expect(bm.toArray()).toEqual([]);
    bm.add(7, 1, 3);
    expect(bm.toArray()).toEqual([1, 3, 7]);
    bm.add();
    expect(bm.toArray()).toEqual([1, 3, 7]);
    bm.remove(5, 7);
    expect(bm.toArray()).toEqual([1, 3]);
    bm.remove();
    expect(bm.toArray()).toEqual([1, 3]);
    bm.clear();
    expect(bm.toArray()).toEqual([]);
});

test('Bitmap / tryAdd, tryRemove', async () => {
    let bm = new Bitmap();
    bm.add(7, 1, 3);
    let n1 = bm.tryAdd(1, 8, 9);
    expect(n1).toEqual(2);
    expect(bm.toArray()).toEqual([1, 3, 7, 8, 9]);
    let n2 = bm.tryRemove(3, 9, 10);
    expect(n2).toEqual(2);
    expect(bm.toArray()).toEqual([1, 7, 8]);
});

test('Bitmap / iterator, count', async () => {
    let bm = newBitmap(1, 6, 10, 2);
    let bits = [];
    for (let bit of bm.iterator()) {
        bits.push(bit);
    }
    expect(bits).toEqual([1, 2, 6, 10]);
    expect(bm.count).toEqual(4);
});

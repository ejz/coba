const utils = require('ejz-utils');

const types = require('./types');

test('types / getIdFromLiteral', () => {
    expect(types.getIdFromLiteral('0')).toEqual(null);
    expect(types.getIdFromLiteral('10')).toEqual(10);
    expect(types.getIdFromLiteral('+1')).toEqual(null);
    expect(types.getIdFromLiteral('-1')).toEqual(null);
    expect(types.getIdFromLiteral('1E3')).toEqual(null);
    expect(types.getIdFromLiteral('110.9')).toEqual(null);
    expect(types.getIdFromLiteral('110.0')).toEqual(null);
    expect(types.getIdFromLiteral('110')).toEqual(110);
    expect(types.getIdFromLiteral('foo')).toEqual(null);
    expect(types.getIdFromLiteral('100000000000000000')).toEqual(null);
});

test('types / makeWordsUnique', () => {
    expect(types.makeWordsUnique('A B B C'.split(' '), false, false)).toEqual([['A', 'B', 'C'], false, false]);
    expect(types.makeWordsUnique('B A B C'.split(' '), true, false)).toEqual([['A', 'B', 'C'], false, false]);
});

test('types / getWordsFromFulltext / 1', () => {
    let cases = [
        [false, 'asd', false, false, ['asd'], false],
        [false, 'asd foo asd bar', false, false, ['asd', 'foo', 'bar'], false],
        [true, 'asd foo asd bar', false, false, ['foo', 'asd', 'bar'], false],
        [false, 'asd bar foo bar', true, false, ['asd', 'bar', 'foo'], false],
        [false, 'asd bar foo !', true, false, ['asd', 'bar', 'foo'], false],
        [true, 'привет asd', false, false, ['asd'], false],
        [false, 'asd привет', true, false, ['asd'], false],
        [false, 'asd1_asd2', false, false, ['asd1', 'asd2'], false],
        [false, 'a_b_c', false, false, ['a', 'b', 'c'], false],
        [false, 'a_b_c_b', false, false, ['a', 'b', 'c'], false],
        [false, 'aaa_bbb_ccc_bbb', false, false, ['aaa', 'bbb', 'ccc'], false],
        [false, 'a_b_c_b', true, false, ['a', 'b', 'c'], false],
        [true, 'b_a_b_c', false, false, ['a', 'b', 'c'], false],
    ];
    for (let [allowPrefix, fulltext, allowPostfix, _allowPrefix, words, _allowPostfix] of cases) {
        expect(types.getWordsFromFulltext(fulltext, allowPrefix, allowPostfix)).toEqual([
            words,
            _allowPrefix,
            _allowPostfix,
        ]);
    }
});

test('types / genWords / 1', async () => {
    expect(types.genWords('s$').includes('0s$')).toEqual(true);
    expect(types.genWords('s$').includes('$s$')).toEqual(true);
    expect(types.genWords('o').includes('$o$')).toEqual(true);
    expect(types.genWords('oo').includes('oo1')).toEqual(true);
    expect(types.genWords('oo').includes('1oo')).toEqual(true);
    expect(types.genWords('o1').includes('$o1')).toEqual(true);
    expect(types.genWords('o1').includes('o1$')).toEqual(true);
    let gen1 = types.genWords('oo');
    expect(gen1.length).toEqual(utils.unique(gen1).length);
    let gen2 = types.genWords('o1');
    expect(gen2.length).toEqual(utils.unique(gen2).length);
    let gen3 = types.genWords('o');
    expect(gen3.length).toEqual(utils.unique(gen3).length);
});

// test('utils / expandInt / 1', async () => {
//     expect(utils.expandInt(6, 2)).toEqual([
//         parseInt('0000011000', 2),
//         parseInt('0000011011', 2),
//     ]);
//     expect(utils.expandInt(255, 0)).toEqual([
//         parseInt('11111111', 2),
//         parseInt('11111111', 2),
//     ]);
// });

// test('utils / convertDateTimeToNumber', async () => {
//     expect(utils.convertDateTimeToNumber(String(1e6))).toEqual(null);
//     expect(utils.convertDateTimeToNumber('1971')).toEqual(365 * 3600 * 24);
//     expect(utils.convertDateTimeToNumber('1972', '1971')).toEqual(365 * 3600 * 24);
//     expect(utils.convertDateTimeToNumber('2020-08-02', '2020-08-01')).toEqual(24 * 3600);
//     expect(utils.convertDateTimeToNumber('2020-08-02', '2020-08-01 00:00:00')).toEqual(24 * 3600);
//     expect(utils.convertDateTimeToNumber('2020-08-02', '2020-08-01 12:00:00')).toEqual(12 * 3600);
//     expect(utils.convertDateTimeToNumber('2020-08-02', '2020-08-01 23:00:00')).toEqual(3600);
//     expect(utils.convertDateTimeToNumber('2020-08-02', '2020-08-01 23:59:00')).toEqual(60);
//     expect(utils.convertDateTimeToNumber('2020-08-02', '2020-08-01 23:59:59')).toEqual(1);
//     expect(utils.convertDateToNumber('2020-08-02', '2020-07-31')).toEqual(2);
// });

// test('utils / convertDateToNumber', async () => {
//     expect(utils.convertDateToNumber(String(1e6))).toEqual(null);
//     expect(utils.convertDateToNumber('1970-02-01')).toEqual(31);
// });

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

test('types / normalizeText', async () => {
    let notation2word = (notation) => {
        return {
            word: notation.replace(/(^\*|\*$)/g, ''),
            allowPrefix: notation.startsWith('*'),
            allowPostfix: notation.endsWith('*'),
        };
    };
    let cases = [
        ['*asd*', '*asd*'],
        ['* asd*', 'asd*'],
        ['*!asd*', 'asd*'],
        ['* ! *', []],
        ['*!*', []],
        ['* 1 *', '1'],
        ['*1*', '*1*'],
        ['**', []],
        ['*hi i\'m samatha*', '*hi,im,samatha*'],
        ['A B B C', 'a,b,c'],
        ['*B A B C', 'a,b,c'],
        ['D C B C*', 'd,c,b'],
        ['*A A A*', 'a'],
        ['*A*', '*a*'],
        ['*A a*', '*a*'],
        ['*A a', 'a'],
    ];
    for (let [inp, out] of cases) {
        let res = types.normalizeText(notation2word(inp).word, notation2word(inp));
        expect(res).toEqual((Array.isArray(out) ? out : out.split(',')).map(notation2word));
    }
});

test('types / normalizeTexts', async () => {
    let notation2word = (notation) => {
        return {
            word: notation.replace(/(^\*|\*$)/g, ''),
            allowPrefix: notation.startsWith('*'),
            allowPostfix: notation.endsWith('*'),
        };
    };
    let cases = [
        ['*as*d*', '*as*d*'],
        ['* a*b*c', 'a*b*c'],
        ['* a *b*c', 'a,*b*c'],
        ['* ! *b*c', '*b*c'],
        ['a* ! *b*c', 'a*b*c'],
        ['a* d *b*c', 'a*,d,*b*c'],
        ['!*!', []],
        [' ! *b*c', '*b*c'],
        ['* ! * ! *a', '*a'],
        ['*a*b c*d*', '*a*b,c*d*'],
        ['a*!*b', 'a*b'],
        ['a*!c!*b', 'a*,c,*b'],
        ['!*b', '*b'],
        ['b*!', 'b*'],
    ];
    for (let [inp, out] of cases) {
        let res = types.normalizeTexts(notation2word(inp).word, notation2word(inp));
        res = res.map((r) => (delete r.placeholder, r));
        expect(res).toEqual((Array.isArray(out) ? out : out.split(',')).map(notation2word));
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

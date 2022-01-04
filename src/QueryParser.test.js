const utils = require('ejz-utils');

const {Terms} = require('./Terms');
const {QueryParser} = require('./QueryParser');

test('QueryParser / tokenize', async () => {
    let qp = new QueryParser();
    let cases = [
        ['"asd"', 'ENTER_QUOTE_MODE QUOTED_VALUE EXIT_QUOTE_MODE'],
        ['1', 'NUMERIC'],
        ['@f', 'IDENT_ALONE'],
        ['@f:a', 'IDENT VALUE'],
        ['@@_g', 'FK_ALONE'],
        ['@@_g:a', 'FK VALUE'],
        [':', null],
        [',', 'SPECIAL'],
        ['1.1', 'NUMERIC'],
        ['+1.1', 'NUMERIC'],
        ['-1.1', 'NUMERIC'],
        ['-1', 'NUMERIC'],
        ['+1', 'NUMERIC'],
        ['1', 'NUMERIC'],
        ['+1.1', 'NUMERIC'],
        ['+1.1_', null],
        ['+1.1 _', 'NUMERIC SPACE VALUE'],
        ['1d', 'VALUE'],
        ['+1d', null],
        ['d+1', 'VALUE NUMERIC'],
        ['%', null],
        ['^', null],
        ['$', null],
        ['[ ( ,', 'RANGE_OPEN SPACE SPECIAL SPACE SPECIAL'],
    ];
    for (let [k, v] of cases) {
        let t;
        try {
            let tokens = qp.tokenize(k);
            t = tokens.map((t) => t.type).join(' ');
        } catch (err) {
            t = null;
        }
        expect(t).toEqual(v);
    }
});

test('QueryParser / normalizeMergeQuote', async () => {
    let qp = new QueryParser();
    let cases = [
        ['"a"', 'a'],
        ['""', ''],
        ['"\\""', '"'],
    ];
    for (let [k, v] of cases) {
        let tokens = qp.tokenize(k);
        tokens = qp.normalize(tokens, 2);
        let [token] = tokens;
        expect(token.type).toEqual('VALUE');
        expect(token.value).toEqual(v);
    }
});

test('QueryParser / normalizeMergeRange', async () => {
    let qp = new QueryParser();
    let cases = [
        ['a [ ( "1" , "2" ) ] @a', [true, '1', '2', true]],
        ['[ ]', null],
        ['[()]', null],
        ['[[', null],
        [']]', null],
        ['[,]', [false, null, null, false]],
        ['[(1,"+1"]', [true, '1', '+1', false]],
        ['[("","+1"]', [true, '', '+1', false]],
        ['[(1,]', [true, '1', null, false]],
        ['[(,)]', null],
        ['[(a,)]', null],
        ['[(,a)]', null],
        ['[(a,b)]', [true, 'a', 'b', true]],
        [' [ ( a , b ) ] ', [true, 'a', 'b', true]],
        ['[*,1]', null],
        ['[1,*]', null],
        ['["*",1]', [false, '*', '1', false]],
        ['[1,"*"]', [false, '1', '*', false]],
        ['[1]', [false, '1', '1', false]],
        ['[ (1]', null],
        ['[1) ]', null],
    ];
    for (let [q, res] of cases) {
        let token = null;
        try {
            let tokens = qp.tokenize(q);
            tokens = qp.normalize(tokens, 3);
            token = true;
            token = tokens.find((t) => Array.isArray(t.value)).value;
        } catch (e) {
        }
        expect(token).toEqual(res);
    }
});

test('QueryParser / normalizeMergePrefix', async () => {
    let qp = new QueryParser();
    let cases = [
        ['@a ~val', {value: 'val', allowPostfix: false, allowPrefix: true}],
        ['@a val~', {value: 'val', allowPostfix: true, allowPrefix: false}],
        ['@a ~val~', {value: 'val', allowPostfix: true, allowPrefix: true}],
        ['@a val~', {value: 'val', allowPostfix: true, allowPrefix: false}],
        ['@a ~"val"', {value: 'val', allowPostfix: false, allowPrefix: true}],
        ['@a ~"val"~', {value: 'val', allowPostfix: true, allowPrefix: true}],
        ['@a "val"~', {value: 'val', allowPostfix: true, allowPrefix: false}],
        ['@a *~', null],
        ['@a ~*', null],
        ['@a~', null],
        ['~@a', null],
        ['val ~', null],
        ['~ val', null],
    ];
    for (let [q, res] of cases) {
        let tokens, token = null;
        try {
            tokens = qp.tokenize(q);
            tokens = qp.normalize(tokens, 4);
            token = true;
            token = tokens.find((t) => t.type == 'VALUE').value;
        } catch (e) {
        }
        if (token != null) {
            expect(tokens.findIndex(({type}) => type == 'PREFIX')).toEqual(-1);
        }
        expect(token).toEqual(res);
    }
});

test('QueryParser / normalizeMergeIdentCompare', async () => {
    let qp = new QueryParser();
    let cases = [
        ['@a > ( 1 )', [true, '1', null, false]],
        ['@a > "foo"', [true, 'foo', null, false]],
        ['@a > "1"', [true, '1', null, false]],
        ['@a <= "1"', [false, null, '1', false]],
        ['@a <= ( (( ("1") )) )', [false, null, '1', false]],
        ['@a>=', null],
        ['@a>=@a', null],
        ['@a@a', null],
        ['foo"bar"', null],
        ['_+1', null],
        ['_-1', null],
        ['"a"a', null],
        ['"a"1', null],
        ['@a@b', null],
        ['1@a', null],
        ['1@a', null],
        ['@@b@a', null],
        ['2@@b', null],
        ['@@b[1,2]', null],
        ['@a>[1]', null],
        ['@a>[,]', null],
        ['@a>*', null],
        ['@a>~"foo"', null],
        ['[1,1]2', null],
        ['[1,1]+2', null],
        ['[1,1][2,1]', null],
        ['(@a @a>(1))', [true, '1', null, false]],
        ['(@a @a>(((1))))', [true, '1', null, false]],
    ];
    for (let [q, res] of cases) {
        let token = null;
        try {
            let tokens = qp.tokenize(q);
            tokens = qp.normalize(tokens, 5);
            token = true;
            token = tokens.find((t) => t.type == 'VALUE').value;
        } catch (e) {
        }
        expect(token).toEqual(res);
    }
});

test('QueryParser / tokens2infix / 1', async () => {
    let infix = (query) => {
        let qp = new QueryParser();
        let tokens = qp.tokenize(query);
        tokens = qp.normalize(tokens);
        return qp.tokens2infix(tokens, new Terms());
    };
    let cases = [
        ['@a', '2'],
        ['@a (@a)', '2 & 2'],
        ['@a:*', '2'],
        ['@a:(a|b)', '( 2 | 3 )'],
        ['"value" & @a:value', '3 & 2'],
        ['@a @b', '2 & 3'],
        ['(@a) (@b)', '2 & 3'],
        ['@fk:1', '2'],
        ['@a:(a|b) @a:(a|b) @c:(a|b)', '( 2 | 3 ) & ( 2 | 3 ) & ( 4 | 5 )'],
        [':', null],
        ['@', null],
        ['@a:@a', null],
        ['~ a', null],
        ['~ 1', null],
        ['+1.1_', null],
        ['+1d', null],
        ['d+1', null],
        ['*@a', null],
        ['@a*', null],
        ['**', null],
        ['@a:[(,]', null],
        ['@a:[,)]', null],
        ['((@a)', null],
        ['(@a))', null],
        ['@a -@b', '2 & - 3'],
        ['@a --@b', '2 & - - 3'],
        ['@a -(-@b)', '2 & - ( - 3 )'],
        [
            new Array(1E4).fill('( @f1:10 )').join(' '),
            new Array(1E4).fill('( 2 )').join(' & '),
        ],
    ];
    for (let [q, ret] of cases) {
        if (ret == null) {
            expect(() => infix(q)).toThrow();
            continue;
        }
        expect(infix(q)).toEqual(ret);
    }
});

test('QueryParser / tokens2infix / 2', async () => {
    let infix = (query, terms) => {
        let qp = new QueryParser();
        let tokens = qp.tokenize(query);
        tokens = qp.normalize(tokens);
        return qp.tokens2infix(tokens, terms ?? new Terms());
    };
    let cases = [
        ['@@fk', '2', '1'],
        ['(@@fk)', '2', '1'],
        ['@@fk:1', '3', '2'],
        ['@@fk:@a:1', '3', '2'],
        ['@@fk:(@a:(1 | 2))', '4', '( 2 | 3 )'],
        ['@@fk:@@fk:@@fk:@@fk', '5', '4'],
    ];
    for (let [q, ret, inf] of cases) {
        if (ret == null) {
            expect(() => infix(q)).toThrow();
            continue;
        }
        let terms = new Terms();
        let res = infix(q, terms);
        expect(res).toEqual(ret);
        let i = (res.match(/\d+/g) || []).find((t) => terms.get(t).infix != null);
        expect(terms.get(i).infix).toEqual(inf);
    }
});

test('QueryParser / optimizePostfix', async () => {
    let cases = [
        ['1 -', '0'],
        ['1 - -', '1'],
        ['0 -', '1'],
        ['1 2 3 4 5 6 & & & & &', '1 2 3 4 5 6 &6'],
        ['1 11 &2 2 3 4 5 6 &6', '1 11 2 3 4 5 6 &7'],
        ['1 11 |2 2 3 4 5 6 &6', '1 11 |2 2 3 4 5 6 &6'],
        ['1 2 3 - 4 5 6 & & & & &', '1 2 -3 4 5 6 &6'],
        ['1 2 & 3 & 1 2 | 3 | &', '1 2 3 &3 1 2 3 |3 &2'],
        ['1 - - - - 2 &2', '1 2 &2'],
    ];
    for (let [inp, out] of cases) {
        let qp = new QueryParser();
        let postfix = qp.optimizePostfix(inp);
        expect(postfix).toEqual(out);
    }
});

test('QueryParser / infix2postfix', async () => {
    let qp = new QueryParser();
    let cases = [
        ['3 & ( 1 | 2 )', '3 1 2 |2 &2'],
        ['- ( 2 & 3 )', '2 3 &2 -'],
        ['- ( 2 | 3 )', '2 3 |2 -'],
        ['( - ( 2 ) )', '-2'],
        ['1 & 2 & - ( 3 )', '1 2 -3 &3'],
        ['- ( 1 & 2 & - ( 3 ) )', '1 2 -3 &3 -'],
    ];
    for (let [inp, out] of cases) {
        expect(qp.infix2postfix(inp)).toEqual(out);
    }
});

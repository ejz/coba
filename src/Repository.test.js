const utils = require('ejz-utils');

const {Repository} = require('./Repository');

const {
    makeRepository,
    onShutdown,
} = require('./di');

function getRepository(fields, name) {
    return makeRepository({name, fields});
}

afterEach(onShutdown);

test('Repository / nextid', () => {
    let r = getRepository();
    expect(r.nextid()).toEqual(1);
    expect(r.nextid()).toEqual(2);
    expect(r.nextid()).toEqual(3);
});

test('Repository / bare', () => {
    let r1 = getRepository({f: {type: 'Boolean'}});
    expect(r1.bare.values).toEqual({f: null});
    let r2 = getRepository({f: {type: 'Counter', child: 'child'}});
    expect(r2.bare.counters).toEqual({f: 0});
});

test('Repository / insert, count, min, max, has, delete, get', () => {
    let r = getRepository({f: {type: 'Boolean'}});
    expect(r.min()).toEqual(0);
    expect(r.max()).toEqual(0);
    expect(r.count()).toEqual(0);
    expect(r.has(1)).toEqual(false);
    let id = r.insert({f: true});
    expect(id).toEqual(1);
    let rec2 = r.get(1);
    expect(rec2).toEqual({f: true});
    let rec3 = r.get(10);
    expect(rec3).toEqual(null);
    let rec4 = r.get(1, []);
    expect(rec4).toEqual({});
    r.insert({f: false});
    expect(r.min()).toEqual(1);
    expect(r.max()).toEqual(2);
    expect(r.count()).toEqual(2);
    expect(r.has(1)).toEqual(true);
    expect(r.delete(10)).toEqual(false);
    expect(r.delete(1)).toEqual(true);
    expect(r.has(1)).toEqual(false);
});

test('Repository / index field / 2', () => {
    let r = getRepository({f: {type: 'NumberIndex', min: 5}});
    r.insert({f: true});
    r.insert({f: 4});
    r.insert({f: 6});
    expect(r.get(1)).toEqual({});
    expect(r.get(2)).toEqual({});
    expect(r.get(3)).toEqual({f: 6});
});

test('Repository / iterate / 1', () => {
    let r = getRepository({i: {type: 'Number', min: 5}});
    for (let i = 1; i <= 100; i++) {
        r.insert({i});
    }
    let recs = [...r.iterate()];
    for (let i = 1; i <= 100; i++) {
        expect(recs[i - 1]).toEqual([i, i < 5 ? {} : {i}]);
    }
});

test('Repository / iterate / 2', () => {
    let r = getRepository();
    r.insert(); // 1
    r.insert(); // 2
    r.insert(); // 3
    let cases = [
        ['*', [1, 2, 3]],
        ['* | -*', [1, 2, 3]],
        ['* & -*', []],
        ['[,]', [1, 2, 3]],
        ['[1]', [1]],
        ['["1"]', [1]],
        ['["a"]', []],
        ['[100]', []],
        ['[""]', []],
        ['[1.0]', []],
        ['["+1"]', []],
        ['[+1]', []],
        ['[,"2"]', [1, 2]],
        ['[1,"+2"]', []],
        ['[1,"2"]', [1, 2]],
        ['[(1,]', [2, 3]],
        ['[,]', [1, 2, 3]],
        ['[(1,3)]', [2]],
        ['-[2]', [1, 3]],
        ['-[,]', []],
        ['-[2,2]', [1, 3]],
        ['([1] | [2] | ["a"])', [1, 2]],
        ['[1,2] & [2,3] & [,]', [2]],
    ];
    for (let [query, res] of cases) {
        expect([...r.iterate(query)].map(([id]) => id)).toEqual(res);
    }
});

test('Repository / iterate / random', () => {
    let r = getRepository({i: {type: 'Number', min: 1}});
    let cases = [
        [[1, 1, 100], [[1, 1]]],
        [[1, 10, 2], [[1, 5], [6, 10]]],
        [[1, 3, 2], [[1, 1], [2, 3]]],
        [[1, 3, 10], [[1, 1], [2, 2], [3, 3]]],
        [[1, 3, 1], [[1, 3]]],
        [[1, 10, 3], [[1, 3], [4, 6], [7, 10]]],
    ];
    for (let [args, ret] of cases) {
        let _ret = r.splitInterval(...args);
        expect(_ret).toEqual(ret);
    }
    for (let i = 1; i <= 10E3; i++) {
        r.insert({i});
    }
    let recs = [...r.iterate(null, null, null, null, null, true)].map(([id]) => id);
    expect(recs[0]).not.toEqual(1);
    expect(recs[1]).not.toEqual(2);
});

test('Repository / fulltext / 1', () => {
    let r = getRepository({text: {type: 'Fulltext'}});
    r.insert({text: '1'}); // 1
    r.insert({text: 'hello world'}); // 2
    r.insert({text: 'foo bar'}); // 3
    r.insert({text: 'moo foo'}); // 4
    r.insert({text: 'ba baz'}); // 5
    r.insert({text: 2}); // 6
    let cases = [
        ['*', [1, 2, 3, 4, 5, 6]],
        ['@text:"hello"', [2]],
        ['@text:"hello" @text:"world"', [2]],
        ['@text:hello', [2]],
        ['@text:"hello world"', [2]],
        ['@text:"world hello"', [2]],
        ['@text:foo -@text:bar', [4]],
        ['hello', [2]],
    ];
    for (let [query, res] of cases) {
        expect([...r.iterate(query)].map(([id]) => id)).toEqual(res);
    }
});

test('Repository / fulltext / 2', () => {
    let r = getRepository({text: {type: 'Fulltext'}});
    r.insert({text: 'foo'}); // 1
    r.insert({text: 'food'}); // 2
    r.insert({text: 'bar baz'}); // 3
    r.insert({text: 'm n p o'}); // 4
    let cases = [
        ['foo', [1]],
        ['m n', [4]],
        ['foo~', [1, 2]],
        ['fo~', [1, 2]],
        ['f~', [1, 2]],
        ['o~', [4]],
        ['~oo~', [1, 2]],
        ['~a~', [3]],
        ['"bar ba"~', [3]],
        ['"ba ba"~', []],
        ['fo~ & -foo', [2]],
        ['fo~ -foo', [2]],
        ['baz', [3]],
        ['bar baz', [3]],
        ['baz bar', [3]],
        ['~m~', [4]],
        ['~oo~', [1, 2]],
        ['"baz bar"', [3]],
        ['"bar baz"', [3]],
    ];
    for (let [query, res] of cases) {
        expect([...r.iterate(query)].map(([id]) => id)).toEqual(res);
    }
});

test('Repository / fulltext / 3', () => {
    let r = getRepository({text1: {type: 'Fulltext'}, text2: {type: 'Fulltext'}});
    r.insert({text1: 'foo bar', text2: 'baz moo'}); // 1
    expect([...r.iterate('foo moo')].map(([id]) => id)).toEqual([1]);
    expect([...r.iterate('baz bar')].map(([id]) => id)).toEqual([1]);
    expect([...r.iterate('@text1:"baz bar"')].map(([id]) => id)).toEqual([]);
    expect([...r.iterate('@text2:"moo baz"')].map(([id]) => id)).toEqual([1]);
});

test('Repository / fulltext notnull case', () => {
    let r = getRepository({text: {type: 'Fulltext'}});
    expect(() => r.insert({text: 2})).not.toThrow();
    expect(() => r.insert({})).not.toThrow();
    expect(() => r.iterate('@text')).toThrow();
    r = getRepository({text: {type: 'Fulltext', notnull: true}});
    expect(() => r.insert({text: 2})).toThrow();
    expect(() => r.insert({})).toThrow();
    r.insert({text: 'hi'}); // 1
    expect([...r.iterate('@text')].map(([id]) => id)).toEqual([1]);
    r = getRepository({text: {type: 'Fulltext', trackall: true}});
    expect(() => r.insert({text: 2})).not.toThrow();
    expect(() => r.insert({})).not.toThrow();
    r.insert({text: 'hi'}); // 3
    expect([...r.iterate('@text')].map(([id]) => id)).toEqual([3]);
});

test('Repository / types / 1', () => {
    let cases = [
        [
            {a: {type: 'Number', notnull: true}, b: {type: 'Number'}},
            [
                [undefined, true],
                [{}, true],
                [{a: null}, true],
                [{a: undefined}, true],
                [{a: 'b'}, true],
                [{a: ''}, true],
                [{a: 'b.1'}, true],
                [{a: '1.b'}, true],
                [{a: Number('1'.repeat(1E3))}, true],
                [{a: 0}, false, {a: 0}],
                [{a: 0, b: '1'}, false, {a: 0}],
                [{a: 0, b: 1}, false, {a: 0, b: 1}],
                [{a: 0, b: null}, false, {a: 0}],
            ],
        ],
        [
            {a: {type: 'NumberIndex', min: 1, max: 10, notnull: true}, b: {type: 'NumberIndex', min: 5, max: 10.5, precision: 1}},
            [
                [undefined, true],
                [{}, true],
                [{a: 0}, true],
                [{a: 1}, false, {a: 1}],
                [{a: 9, b: 'foo'}, false, {a: 9}],
                [{a: 1, b: 10.19}, false, {a: 1, b: 10.1}],
                [{a: 1, b: 10.59}, false, {a: 1, b: 10.5}],
                [{a: 1, b: 10.5}, false, {a: 1, b: 10.5}],
                [{a: 1, b: 10.4}, false, {a: 1, b: 10.4}],
                [{a: 1, b: 8.8}, false, {a: 1, b: 8.8}],
                [{a: 1, b: 10.6}, false, {a: 1}],
            ],
        ],
        [
            {a: {type: 'String'}},
            [
                [undefined, false, {}],
                [{}, false, {}],
                [{a: 0}, false, {}],
                [{a: '1'}, false, {a: '1'}],
                [{b: '1'}, true],
            ],
        ],
        [
            {a: {type: 'Boolean'}},
            [
                [undefined, false, {}],
                [{}, false, {}],
                [{a: 0}, false, {}],
                [{a: true}, false, {a: true}],
                [{a: false}, false, {a: false}],
            ],
        ],
        [
            {a: {type: 'Fulltext'}},
            [
                [undefined, false, {}],
                [{}, false, {}],
                [{a: 0}, false, {}],
                [{a: 'true'}, false, {a: 'true'}],
                [{a: 'foo bar true'}, false, {a: 'foo bar true'}],
            ],
        ],
        [
            {a: {type: 'BooleanIndex'}},
            [
                [undefined, false, {}],
                [{}, false, {}],
                [{a: 0}, false, {}],
                [{a: true}, false, {a: true}],
                [{a: false}, false, {a: false}],
            ],
        ],
        [
            {a: {type: 'StringIndex'}},
            [
                [undefined, false, {}],
                [{}, false, {}],
                [{a: 0}, false, {}],
                [{a: '1'}, false, {a: '1'}],
                [{b: '1'}, true],
                [{a: '1'.repeat(1E3)}, false, {a: '1'.repeat(1E3)}],
            ],
        ],
        [
            {d1: {type: 'Date', notnull: true}, d2: {type: 'Date'}},
            [
                [undefined, true],
                [{}, true],
                [{d1: null}, true],
                [{d1: undefined}, true],
                [{d1: 'asd'}, true],
                [{d1: '2020'}, false, {d1: '2020-01-01'}],
                [{d1: '2020-06'}, false, {d1: '2020-06-01'}],
                [{d1: '2020-08-04'}, false, {d1: '2020-08-04'}],
                [{d1: '2020-08-04 20:00:00'}, false, {d1: '2020-08-04'}],
                [{d1: '2020-08-01', d2: '1000000'}, false, {d1: '2020-08-01'}],
            ],
        ],
        [
            {dt: {type: 'Datetime'}},
            [
                [{dt: 'asd'}, false, {}],
                [{dt: '2020'}, false, {dt: '2020-01-01 00:00:00'}],
                [{dt: '2020-90-90'}, false, {}],
                [{dt: '2020-05-20'}, false, {dt: '2020-05-20 00:00:00'}],
                [{dt: '2020-05-21 01:02:03'}, false, {dt: '2020-05-21 01:02:03'}],
            ],
        ],
        [
            {a: {type: 'BooleanArray'}},
            [
                [{a: true}, false, {a: [true]}],
                [{a: []}, false, {a: []}],
                [{a: [1]}, false, {}],
                [{a: [true]}, false, {a: [true]}],
                [{a: [true, false, true]}, false, {a: [true, false, true]}],
            ],
        ],
        [
            {a: {type: 'StringArray'}},
            [
                [{a: 'asd'}, false, {a: ['asd']}],
                [{a: []}, false, {a: []}],
                [{a: [1]}, false, {}],
                [{a: ['a']}, false, {a: ['a']}],
                [{a: ['a', null, 'b']}, false, {}],
                [{a: ['a', 'b']}, false, {a: ['a', 'b']}],
            ],
        ],
        [
            {a: {type: 'NumberArray'}},
            [
                [{a: '1'}, false, {}],
                [{a: 1}, false, {a: [1]}],
                [{a: []}, false, {a: []}],
                [{a: [1]}, false, {a: [1]}],
            ],
        ],
        [
            {a: {type: 'DateArray'}},
            [
                [{a: '2020'}, false, {a: ['2020-01-01']}],
                [{a: 1}, false, {}],
                [{a: []}, false, {a: []}],
            ],
        ],
        [
            {a: {type: 'DatetimeArray'}},
            [
                [{a: '2020'}, false, {a: ['2020-01-01 00:00:00']}],
                [{a: '2020-01-02 03:04:05'}, false, {a: ['2020-01-02 03:04:05']}],
                [{a: 1}, false, {}],
                [{a: []}, false, {a: []}],
            ],
        ],
        [
            {a: {type: 'Enum', values: ['one', 'two']}},
            [
                [{a: 'One'}, false, {a: 'ONE'}],
                [{a: ''}, false, {}],
                [{a: 'three'}, false, {}],
                [{a: 1}, false, {}],
                [{a: 'two'}, false, {a: 'TWO'}],
            ],
        ],
        [
            {a: {type: 'Object'}},
            [
                [{a: 'One'}, false, {}],
                [{a: {}}, false, {a: {}}],
                [{a: {a: 1, b: 'foo', c: []}}, false, {a: {a: 1, b: 'foo', c: []}}],
            ],
        ],
    ];
    // cases = [cases.pop()];
    for (let [fields, _cases] of cases) {
        let repo = getRepository(fields);
        for (let [values, throws, res] of _cases) {
            if (throws) {
                expect(() => repo.insert(values)).toThrow();
                continue;
            }
            let id = repo.insert(values);
            expect(repo.get(id)).toEqual(res);
        }
    }
});

test('Repository / types / 2', () => {
    let r = getRepository({
        n: {type: 'Number'},
        ni: {type: 'NumberIndex', min: 1, max: 10},
        b: {type: 'Boolean'},
        bi: {type: 'BooleanIndex'},
        s: {type: 'String'},
        si: {type: 'StringIndex'},
    });
    r.insert({n: 1}); // 1
    r.insert({ni: 5}); // 2
    r.insert({b: true}); // 3
    r.insert({bi: false}); // 4
    r.insert({s: 'A'}); // 5
    r.insert({si: 'B'}); // 6
    expect(r.get(2)).toEqual({ni: 5});
    r.update(2, {ni: 6});
    expect(r.get(2)).toEqual({ni: 6});
    r.update(2, {ni: 5});
    expect(r.get(2)).toEqual({ni: 5});
    let cases = [
        ['*', [1, 2, 3, 4, 5, 6]],
        ['@n'],
        ['@b'],
        ['@s'],
        ['@ni', [2]],
        ['@ni:5', [2]],
        ['-@ni:5', [1, 3, 4, 5, 6]],
        ['@ni:[,]', [2]],
        ['@ni:["5",]', [2]],
        ['@ni:["6",]', []],
        ['@ni:["4","6"]', [2]],
        ['@ni:[("4","6")]', [2]],
        ['@bi', [4]],
        ['@bi:"false"', [4]],
        ['@bi:false', [4]],
        ['@si', [6]],
        ['@si:B', [6]],
        ['@si:[B]', null],
        ['@si:A', []],
    ];
    let uniq = (array) => array.filter((v, i, a) => a.indexOf(v) == i);
    let checker = (deleted = 0, updated = 0) => {
        for (let [query, res] of cases) {
            if (!res) {
                expect(() => r.iterate(query)).toThrow();
                continue;
            }
            if (updated && deleted) {
                res = res.filter((id) => id != updated);
                res = res.map((id) => id == deleted ? updated : id);
                res = utils.unique(res);
                res.sort((a, b) => a - b);
            } else if (deleted) {
                res = res.filter((id) => id != deleted);
            }
            expect([...r.iterate(query)].map(([id]) => id)).toEqual(res);
        }
    };
    checker();
    let deleted = utils.rand(1, 6);
    let updated;
    do {
        updated = utils.rand(1, 6);
    } while (updated == deleted);
    let rec = r.get(deleted);
    rec = Object.assign({
        n: null,
        ni: null,
        b: null,
        bi: null,
        s: null,
        si: null,
    }, rec);
    r.delete(deleted);
    checker(deleted);
    r.update(updated, rec);
    checker(deleted, updated);
});

test('Repository / unique / 1', () => {
    let r = getRepository({
        un: {type: 'Unique', fields: ['f']},
        f: {type: 'NumberIndex', min: 1, max: 10, precision: 1},
    });
    let id1 = r.insert({f: 1});
    expect(id1).toEqual(1);
    expect(() => r.insert({f: 1})).toThrow();
    expect([...r.iterate('@f:1')]).toEqual([[1, {f: 1}]]);
    if (Math.round(Math.random())) {
        r.update(1, {f: Math.round(Math.random()) ? null : 10});
    } else {
        r.delete(1);
    }
    expect([...r.iterate('@f:1')]).toEqual([]);
    let id2 = r.insert({f: 1});
    expect(id2).toEqual(2);
    expect(r.get(id2)).toEqual({f: 1});
});

test('Repository / unique / 2', () => {
    let r = getRepository({
        un: {type: 'Unique', fields: ['f']},
        f: {type: 'StringIndex'},
    });
    let id1 = r.insert({f: '1'});
    expect(id1).toEqual(1);
    expect(() => r.insert({f: '1'})).toThrow();
    expect([...r.iterate('@f:1')]).toEqual([[1, {f: '1'}]]);
    if (Math.round(Math.random())) {
        r.update(1, {f: Math.round(Math.random()) ? null : '10'});
    } else {
        r.delete(1);
    }
    expect([...r.iterate('@f:1')]).toEqual([]);
    let id2 = r.insert({f: '1'});
    expect(id2).toEqual(2);
    expect(r.get(id2)).toEqual({f: '1'});
});

test('Repository / unique / 3', () => {
    let r = getRepository({
        un: {type: 'Unique', fields: ['f1', 'f2']},
        f1: {type: 'StringIndex'},
        f2: {type: 'NumberIndex', min: 1, max: 10},
        s: {type: 'StringIndex'},
    });
    expect(r.get(r.insert({f1: '1'}))).toEqual({f1: '1'});
    expect(r.get(r.insert({f2: 1}))).toEqual({f2: 1});
    let id = r.insert({f1: 'A', f2: 10});
    expect(r.get(id)).toEqual({f1: 'A', f2: 10});
    r.update(id, {f1: 'B', f2: 9});
    expect(r.get(id)).toEqual({f1: 'B', f2: 9});
    r.update(id, {f1: 'B', f2: 11});
    expect(r.get(id)).toEqual({f1: 'B'});
    r.update(id, {f2: 8});
    expect(r.get(id)).toEqual({f1: 'B', f2: 8});
    r.update(id, {f1: 'B', f2: 7, s: 's'});
    expect(r.get(id)).toEqual({f1: 'B', f2: 7, s: 's'});
    r.update(id, {f1: 'C', s: 's1'});
    expect(r.get(id)).toEqual({f1: 'C', f2: 7, s: 's1'});
    r.update(id, {f2: 6, s: 's2'});
    expect(r.get(id)).toEqual({f1: 'C', f2: 6, s: 's2'});
    if (Math.round(Math.random())) {
        r.update(id, {f2: null, s: 's3'});
        expect(r.get(id)).toEqual({f1: 'C', s: 's3'});
    } else {
        r.update(id, {f1: null, s: 's3'});
        expect(r.get(id)).toEqual({f2: 6, s: 's3'});
    }
});

test('Repository / unique / 4', () => {
    let r = getRepository({
        un: {type: 'Unique', fields: ['f']},
        f: {type: 'StringIndex'},
    });
    let id1 = r.insert({f: '1'});
    expect(id1).toEqual(1);
    expect(() => r.insert({f: '1'})).toThrow();
    expect([...r.iterate('@f:1')]).toEqual([[1, {f: '1'}]]);
    if (Math.round(Math.random())) {
        r.update(1, {f: Math.round(Math.random()) ? null : String(10)});
    } else {
        r.delete(1);
    }
    expect([...r.iterate('@f:1')]).toEqual([]);
    let id2 = r.insert({f: '1'});
    expect(id2).toEqual(2);
    expect(r.get(id2)).toEqual({f: '1'});
});

test('Repository / unique / 5', async () => {
    let r = getRepository({
        uniq: {
            type: 'Unique',
            fields: ['a', 'b'],
        },
        a: {
            type: 'NumberIndex',
            min: 1,
            max: 4,
        },
        b: {
            type: 'NumberIndex',
            min: 1,
            max: 4,
        },
    });
    let vals = {};
    for (let i = 1; i <= 100; i++) {
        let a = utils.rand(0, 10);
        let b = utils.rand(0, 10);
        let id;
        try {
            id = r.insert({a, b});
        } catch (e) {
            continue;
        }
        a = (1 <= a && a <= 4) ? a : null;
        b = (1 <= b && b <= 4) ? b : null;
        vals[id] = {a, b};
    }
    let getter = (filter) => {
        let ret = utils.filter(vals, filter);
        ret = utils.map(ret, (v) => utils.filter(v, (k, v) => v != null));
        return Object.entries(ret).sort(([id1], [id2]) => {
            return (+id1) - (+id2);
        }).map(([k, v]) => [+k, v]);
    };
    let cases = [
        ['*', () => true],
        ['[1]', (k) => k == 1],
        ['@a:1', (k, v) => v.a == 1],
        ['-@a:1', (k, v) => v.a != 1],
        ['-@a:1 @a', (k, v) => v.a != 1 && v.a != null],
        ['@a:1 @b:2', (k, v) => v.a == 1 && v.b == 2],
        ['@a:1 @b:(2|3)', (k, v) => v.a == 1 && (v.b == 2 || v.b == 3)],
        ['@a:(1|2) @b:3', (k, v) => v.b == 3 && (v.a == 1 || v.a == 2)],
        ['@a @b', (k, v) => v.a != null && v.b != null],
        ['@a | @b', (k, v) => v.a != null || v.b != null],
        ['-(@a:1 @b:2)', (k, v) => !(v.a == 1 && v.b == 2)],
    ];
    for (let [q, filter] of cases) {
        expect([...r.iterate(q)]).toEqual(getter(filter));
    }    
});

test('Repository / parent', () => {
    let parent = getRepository({s: {type: 'String'}}, 'parent');
    let child = getRepository({
        parent_id: {type: 'Parent', parent: 'parent'},
    }, 'child');
    child.repositories = {parent};
    parent.repositories = {child};
    let p1 = parent.insert({s: 'foo'});
    child.insert();
    child.insert();
    child.insert();
    child.insert();
    let c1 = child.insert({parent_id: p1});
    expect(child.get(c1)).toEqual({parent_id: p1});
    expect([...parent.iterate(`@@child:([${c1}])`)]).toEqual([[p1, {s: 'foo'}]]);
    expect([...child.iterate(`@@parent:([${p1}])`)]).toEqual([[c1, {parent_id: p1}]]);
});

test('Repository / counter / 1', async () => {
    let parent = getRepository({s: {type: 'String'}, cnt: {
        type: 'Counter',
        child: 'child',
    }}, 'parent');
    let child = getRepository({
        parent_id: {type: 'Parent', parent: 'parent'},
    }, 'child');
    child.repositories = {parent};
    parent.repositories = {child};
    expect(() => parent.insert({cnt: 0})).toThrow();
    let p1 = parent.insert({s: 'foo'});
    expect(parent.get(p1)).toEqual({s: 'foo', cnt: 0});
    expect([...parent.iterate('@cnt:0')][0]).toEqual([p1, {s: 'foo', cnt: 0}]);
    let cid = child.insert({parent_id: p1});
    expect(parent.get(p1)).toEqual({s: 'foo', cnt: 1});
    expect([...parent.iterate('@cnt:1')][0]).toEqual([p1, {s: 'foo', cnt: 1}]);
    if (Math.round(Math.random())) {
        child.delete(cid);
        expect(parent.get(p1)).toEqual({s: 'foo', cnt: 0});
    } else {
        child.update(cid, {parent_id: null});
        expect(parent.get(p1)).toEqual({s: 'foo', cnt: 0});
        child.update(cid, {parent_id: p1});
        expect(parent.get(p1)).toEqual({s: 'foo', cnt: 1});
        child.update(cid, {parent_id: 10});
        expect(parent.get(p1)).toEqual({s: 'foo', cnt: 0});
        child.update(cid, {parent_id: p1});
        expect(parent.get(p1)).toEqual({s: 'foo', cnt: 1});
    }
});

test('Repository / counter / 2', async () => {
    expect(() => getRepository({cnt: {
        type: 'Counter',
        child: 'child',
        notnull: !utils.rand(0, 1),
    }}, 'parent')).toThrow();
    let rep = getRepository({cnt: {
        type: 'Counter',
        child: 'child',
    }}, 'parent');
    expect(rep.ids === rep.fields.All.cnt.bsi.bitmaps.all).toEqual(true);
});

test('Repository / boolean index as number', () => {
    let r = getRepository({f: {type: 'BooleanIndex'}});
    let id = r.insert({f: true});
    let rec1 = r.get(id);
    expect(rec1).toEqual({f: true});
    r.update(id, {f: false});
    let rec2 = r.get(id);
    expect(rec2).toEqual({f: false});
});

test('Repository / option restrict', () => {
    expect(() => getRepository({f: {type: 'Counter', notnull: true}})).toThrow();
});

test('Repository / BooleanArray', () => {
    let r = getRepository({f: {type: 'BooleanArray'}});
    let id = r.insert({f: true});
    expect(r.get(id)).toEqual({f: [true]});
    r.update(id, {'f[+]': false});
    expect(r.get(id)).toEqual({f: [true, false]});
    r.update(id, {'f[+]': true});
    expect(r.get(id)).toEqual({f: [true, false, true]});
    r.update(id, {'f[-]': false});
    expect(r.get(id)).toEqual({f: [true, true]});
    id = r.insert({});
    r.update(id, {'f[+]': [true, true, false]});
    expect(r.get(id)).toEqual({f: [true, true, false]});
    r.update(id, {'f[+!]': true});
    expect(r.get(id)).toEqual({f: [true, true, true, false]});
    r.update(id, {'f[+]': true});
    expect(r.get(id)).toEqual({f: [true, true, true, false, true]});
    r.update(id, {'f[-!]': true});
    expect(r.get(id)).toEqual({f: [true, true, false, true]});
    r.update(id, {'f[-]': [false, false, false, false, true, true, true]});
    expect(r.get(id)).toEqual({f: []});
});

test('Repository / StringSet', () => {
    let r = getRepository({f: {type: 'StringSet'}});
    let id = r.insert({f: 'foo'});
    expect(r.get(id)).toEqual({f: ['foo']});
    r.update(id, {'f[+]': false});
    expect(r.get(id)).toEqual({f: ['foo']});
    r.update(id, {'f[+]': 'foo'});
    expect(r.get(id)).toEqual({f: ['foo']});
    r.update(id, {'f[+]': 'foo', 'f[-]': 'foo'});
    expect(r.get(id)).toEqual({f: []});
    id = r.insert({f: ['foo', 'boo', 'foo']});
    expect(r.get(id)).toEqual({f: ['foo', 'boo']});
    r.update(id, {'f[+!]': 'boo'});
    expect(r.get(id)).toEqual({f: ['foo', 'boo']});
});

test('Repository / NumberArrayIndex / 1', () => {
    let req = !utils.rand(0, 1);
    let r = getRepository({s: {type: 'NumberArrayIndex', min: 1, max: req ? 5E6 : 5}});
    expect(r.fields.All.s.isDiskIndexRequire).toEqual(req);
    let id1 = r.insert({s: [1, 2]});
    let rec1 = r.get(id1);
    expect([...r.iterate('@s:1')]).toEqual([[id1, {s: [1, 2]}]]);
    expect([...r.iterate('@s:2')]).toEqual([[id1, {s: [1, 2]}]]);
    expect([...r.iterate('@s:2 @s:1')]).toEqual([[id1, {s: [1, 2]}]]);
    r.sync();
    if (!req) {
        r.recordDiskCache.load = null;
        r.delete(id1);
    } else {
        let proxy = r.recordDiskCache.load;
        proxy = proxy.bind(r.recordDiskCache);
        let called = false;
        r.recordDiskCache.load = (...args) => {
            called = true;
            return proxy(...args);
        };
        r.delete(id1);
        expect(called).toEqual(true);
    }
    expect([...r.iterate('@s:1')]).toEqual([]);
});

test('Repository / StringArrayIndex / 1', () => {
    let req = !utils.rand(0, 1);
    let r = getRepository({s: {type: 'StringArrayIndex', lowrank: !req}});
    expect(r.fields.All.s.isDiskIndexRequire).toEqual(req);
    let id1 = r.insert({s: ['1', '2']});
    let rec1 = r.get(id1);
    expect([...r.iterate('@s:1')]).toEqual([[id1, {s: ['1', '2']}]]);
    expect([...r.iterate('@s:2')]).toEqual([[id1, {s: ['1', '2']}]]);
    expect([...r.iterate('@s:2 @s:1')]).toEqual([[id1, {s: ['1', '2']}]]);
    r.sync();
    if (!req) {
        r.recordDiskCache.load = null;
        r.delete(id1);
    } else {
        let proxy = r.recordDiskCache.load;
        proxy = proxy.bind(r.recordDiskCache);
        let called = false;
        r.recordDiskCache.load = (...args) => {
            called = true;
            return proxy(...args);
        };
        r.delete(id1);
        expect(called).toEqual(true);
    }
    expect([...r.iterate('@s:1')]).toEqual([]);
});

test('Repository / StringArrayIndex / 2', () => {
    let r = getRepository({s: {type: 'StringArrayIndex', lowrank: utils.rand([true, false])}});
    let id1 = r.insert({s: ['a', 'b']});
    let rec1 = r.get(id1);
    expect(rec1).toEqual({s: ['a', 'b']});
    r.update(id1, {'s[+]': 'c', 's[-]': 'b'});
    let rec2 = r.get(id1);
    expect(rec2).toEqual({s: ['a', 'c']});
    expect([...r.iterate('@s:a')].length).toEqual(1);
    expect([...r.iterate('@s:c')].length).toEqual(1);
    expect([...r.iterate('@s:b')].length).toEqual(0);
});

test('Repository / NumberSetIndex / 1', () => {
    let r = getRepository({s: {type: 'NumberSetIndex'}});
    let id1 = r.insert({s: 1});
    expect(r.get(id1)).toEqual({s: [1]});
    expect([...r.iterate('@s:1')].length).toEqual(1);
    expect([...r.iterate('@s:2')].length).toEqual(0);
    r.update(id1, {s: 2});
    expect(r.get(id1)).toEqual({s: [2]});
    expect([...r.iterate('@s:1')].length).toEqual(0);
    expect([...r.iterate('@s:2')].length).toEqual(1);
    r.update(id1, {s: [3, 4]});
    expect(r.get(id1)).toEqual({s: [3, 4]});
    expect([...r.iterate('@s:2')].length).toEqual(0);
    expect([...r.iterate('@s:3')].length).toEqual(1);
    expect([...r.iterate('@s:4')].length).toEqual(1);
    r.update(id1, {'s[+]': 5, 's[-]': 4, 's[]': 6});
    expect(r.get(id1)).toEqual({s: [3, 5, 6]});
    expect([...r.iterate('@s:4')].length).toEqual(0);
    expect([...r.iterate('@s:5')].length).toEqual(1);
});

test('Repository / EnumIndex / 1', () => {
    let r = getRepository({s: {type: 'EnumIndex', values: ['one', ' two ']}});
    let id1 = r.insert({s: 'one'});
    expect(r.get(id1)).toEqual({s: 'ONE'});
    expect([...r.iterate('@s:onE')].length).toEqual(1);
    expect(r.get(id1)).toEqual({s: 'ONE'});
});

test('Repository / Secondary / 1', () => {
    let r = getRepository({s: {type: 'Fulltext'}, s1: {type: 'StringIndex', evaluate: 's+"!"'}});
    expect(() => r.insert({s1: ''})).toThrow();
    let id = r.insert({s: 'bla'});
    expect(r.get(id)).toEqual({s: 'bla', s1: 'bla!'});
    r.update(id, {s: 'foo'});
    expect(r.get(id)).toEqual({s: 'foo', s1: 'foo!'});
});

test('Repository / Secondary / 2', () => {
    let r = getRepository({a: {type: 'String'}, b: {type: 'String'}, sec: {type: 'String', evaluate: 'a.toLowerCase()+b.toUpperCase()'}});
    let id1 = r.insert({a: 'a'});
    expect(r.get(id1)).toEqual({a: 'a'});
    let id2 = r.insert({a: 'a', b: 'b'});
    expect(r.get(id2)).toEqual({a: 'a', b: 'b', sec: 'aB'});
    expect(r.get(id2, ['sec'])).toEqual({sec: 'aB'});
    r.update(id1, {b: 'boo'});
    expect(r.get(id1)).toEqual({a: 'a', b: 'boo', sec: 'aBOO'});
    r.update(id2, {b: 'boo'});
    expect(r.get(id2)).toEqual({a: 'a', b: 'boo', sec: 'aBOO'});
});

test('Repository / Secondary / 3', () => {
    let r = getRepository({a: {type: 'String'}, b: {type: 'String'}, sec: {type: 'StringIndex', evaluate: 'a.toLowerCase()+b.toUpperCase()'}});
    let count = (q) => [...r.iterate(q)].length;
    let id1 = r.insert({a: 'a'});
    expect(r.get(id1)).toEqual({a: 'a'});
    expect(count('*')).toEqual(1);
    expect(count('@sec')).toEqual(0);
    r.update(id1, {b: 'boo'});
    expect(count('@sec')).toEqual(1);
    expect(count('@sec:"aBOO"')).toEqual(1);
    r.update(id1, {b: null});
    expect(count('@sec')).toEqual(0);
    expect(count('@sec:"aBOO"')).toEqual(0);
});

test('Repository / Secondary / 4', () => {
    let r = getRepository({a: {type: 'String'}, sec: {type: 'StringIndex', evaluate: 'a.length == 1 ? 1 : String(a.length)'}});
    let id1 = r.insert({a: 'a'});
    expect(r.get(id1)).toEqual({a: 'a'});
    let id2 = r.insert({a: 'aaa'});
    expect(r.get(id2)).toEqual({a: 'aaa', sec: '3'});
});

test('Repository / Secondary / 5', () => {
    let r = getRepository({a: {type: 'String'}, sec: {type: 'StringIndex', evaluate: 'String(a.length)'}, un: {type: 'Unique', fields: ['sec']}});
    let count = (q) => [...r.iterate(q)].length;
    let id1 = r.insert({a: 'a'});
    expect(r.get(id1)).toEqual({a: 'a', sec: '1'});
    expect(() => r.insert({a: 'b'})).toThrow();
    let id2 = r.insert({a: 'aa'});
    expect(() => r.update(id1, {a: 'cc'})).toThrow();
    r.update(id1, {a: 'aaa'});
    expect(r.get(id1)).toEqual({a: 'aaa', sec: '3'});
});

test('Repository / Object / 1', () => {
    let r = getRepository({a: {type: 'Object'}, s: {type: 'String'}});
    let id1 = r.insert({a: {b: 'c', bb: [1]}, s: 'string'});
    expect(r.get(id1)).toEqual({a: {b: 'c', bb: [1]}, s: 'string'});
    expect(r.get(id1, ['a.b', 'a.0', 'a.b.c.d', 's', 'a.bb.0'])).toEqual({
        'a.b': 'c',
        s: 'string',
        'a.bb.0': 1,
    });
});

const path = require('path');
const assert = require('assert').strict;
const fs = require('fs');

const {diff} = require('jest-diff');
const utils = require('ejz-utils');
const {Client} = require('pg');
const coba = require('../src/di'); // todo
const Redis = require('ioredis');
const elasticsearch = require('elasticsearch');

const uncaught = (prefix) => (e) => console.log(prefix, e);

process.on('uncaughtException', uncaught('uncaughtException:'));
process.on('unhandledRejection', uncaught('unhandledRejection:'));

let argv = process.argv.slice(2);
const NOOP = (() => {});

if (argv[0] != null && isNaN(+argv[0])) {
    console.log(
        'node',
        path.basename(__filename, '.js'),
        '[--seed|-s <seed>]',
        '[--filter|-f <filter>]',
        '[--diff|-d]',
        '[--data]',
    );
    return;
}

argv = utils.argv(argv, {
    seed: {type: 'string', default: 'myseed', alias: 's'},
    filter: {type: 'string', default: null, alias: 'f'},
    client: {type: 'string', default: null, alias: 'c'},
    diff: {type: 'boolean', default: false, alias: 'd'},
    data: {type: 'boolean', default: false},
});

const data = generateData(
    (argv._[0] ?? 1000),
    argv.seed,
);
let countries = utils.unique(Object.values(data).map(({country: c}) => c).filter(Boolean));
if (argv.data) {
    console.log(JSON.stringify(data));
    return;
}

const rangeCounter = 100;
const multipleCounter = 100;

const CREATE_INSERT = 'create + insert';
const CHECK_UNIQUE_CONSTRAINT = 'check unique constraint';
const SELECT_BY_PK = 'select using primary key';
const SELECT_ALL = 'select all';
const COUNT_ALL = 'count all';
const SELECT_BY_INDEX = 'select using index';
const COUNT_BY_INDEX = 'count using index';
const SELECT_BY_RANGE = 'select using numeric range';
const COUNT_BY_RANGE = 'count using numeric range';
const SORT_BY_RANGE = 'sort using numeric range';
const SELECT_BY_MULTI = 'complex index search';
const COUNT_BY_MULTI = 'complex index count';
const FULLTEXT = 'fulltext search';
const LOOKAHEAD = 'lookahead search';
const LOOKBEHIND = 'lookbehind search';
const SUBSTRING = 'substring search';
const RANDOM_UPDATE = 'randomly update all';
const RANDOM_DELETE = 'randomly delete all';

const cases = [
    SELECT_BY_PK,
    SELECT_ALL,
    COUNT_ALL,
    SELECT_BY_INDEX,
    COUNT_BY_INDEX,
    SELECT_BY_RANGE,
    COUNT_BY_RANGE,
    SORT_BY_RANGE,
    SELECT_BY_MULTI,
    COUNT_BY_MULTI,
    FULLTEXT,
    LOOKAHEAD,
    LOOKBEHIND,
    SUBSTRING,
    RANDOM_UPDATE,
    RANDOM_DELETE,
];

const clients = [
    getJsonClient(),
    getCobaClient(),
    getPostgresClient(),
    getRedisearchClient(),
].filter((client) => {
    return (client.name == 'json' || argv.client == null || client.name.includes(argv.client));
});

const searches = ['word', 'bar', 'hello', 'zoo'].reduce((a, c) => {
    a.push(`${c}`, `${c}`, `${c}*`, `*${c}`, `*${c}*`);
    return a;
}, []);

const timings = {};

(async () => {
    // CONNECT + PING
    for (let client of clients) {
        await client.connect();
        console.log(client.name, 'connected');
        if (await client.ping().catch(console.log)) {
            console.log(client.name, 'pinged ok');
        } else {
            console.log(client.name, 'pinged err');
            process.exit(1);
        }
    }
    let key;
    // CREATE + INSERT
    key = CREATE_INSERT;
    for (let client of clients) {
        let ms = Number(new Date());
        await client.drop();
        await client.create();
        console.log(client.name, 'inserting ..');
        for (let rec of data) {
            let id = await client.insert(rec);
            if (!id) {
                console.log(client.name, 'invalid behavior');
                process.exit(1);
            }
        }
        timings[key] = timings[key] ?? {};
        timings[key][client.name] = Number(new Date()) - ms;
    }
    // CHECK UNIQUE CONSTRAINT
    key = CHECK_UNIQUE_CONSTRAINT;
    for (let client of clients) {
        let ms = Number(new Date());
        console.log(client.name, 'checking unique constraint ..');
        for (let rec of data) {
            let id = await client.insert(rec);
            if (id) {
                console.log(client.name, 'invalid behavior');
                process.exit(1);
            }
        }
        timings[key] = timings[key] ?? {};
        timings[key][client.name] = Number(new Date()) - ms;
    }
    // CASES
    for (let _case of cases) {
        if (argv.filter != null && !_case.includes(argv.filter)) {
            continue;
        }
        let check = {};
        timings[_case] = timings[_case] ?? {};
        for (let client of clients) {
            if (!client.cases[_case]) {
                continue;
            }
            let ms = Number(new Date());
            console.log(client.name, 'run', `"${_case}"`, 'case');
            let idents = await client.cases[_case]({countries, seed: argv.seed});
            check[client.name] = idents;
            timings[_case][client.name] = Number(new Date()) - ms;
        }
        for (let wrong of checkEqual(check)) {
            timings[_case][wrong] = -1;
        }
    }
    // DISCONNECT
    for (let client of clients) {
        await client.disconnect();
        console.log(client.name, 'disconnected');
    }
    console.table(timings);
})();

return;

function generateData(total, seed) {
    let data = [];
    let idents = {};
    let gen = utils.sgen(seed);
    for (let i = 1; i <= total; i++) {
        let ident = gen('${char}${char}${char}${rand(1,2000)}').toUpperCase();
        if (idents[ident]) {
            continue;
        }
        idents[ident] = true;
        data.push({
            ident,
            name: gen('${word} ${word} ${word} ${word}'),
            country: gen('${rand([country,""])}') || null,
            min: +gen('${rand(1,1E6)}'),
            max: +gen('${rand(1,1E6)}'),
        });
    }
    return data;
}

function checkEqual(dict) {
    let sort = (arr) => {
        if (!Array.isArray(arr)) {
            return;
        }
        if (arr.every((a) => Array.isArray(a))) {
            arr.forEach(sort);
        } else {
            arr.sort();
        }
    };
    let wrong = [];
    let entries = Object.entries(dict);
    let eq = (a, b) => {
        let ja = JSON.stringify(a);
        let jb = JSON.stringify(b);
        if (ja != jb) {
            if (argv.diff) {
                console.log(diff(a, b));
            }
            return false;
        }
        return true;
    };
    for (let i = 1; i < entries.length; i++) {
        let [, v0] = entries[0];
        let [ki, vi] = entries[i];
        sort(v0);
        sort(vi);
        if (!eq(v0, vi)) {
            wrong.push(ki);
        }
    }
    return wrong;
}

function getJsonClient() {
    return {
        get name() {
            return 'json';
        },
        connect() {
        },
        disconnect() {
        },
        async ping() {
            return true;
        },
        drop() {
            this.db = {};
            this.ids = [];
            this.ident2id = {};
        },
        create() {
            this.db = {};
            this.ids = [];
            this.ident2id = {};
        },
        insert(record) {
            if (this.ident2id[record.ident]) {
                return false;
            }
            let id = this.ids.length + 1;
            this.ids.push(id);
            this.db[id] = record;
            this.ident2id[record.ident] = id;
            return true;
        },
        textSearch(searches) {
            let ret = [];
            let pairs = Object.values(this.db).map(({ident: i, name: n}) => [i, ' ' + n + ' ']);
            for (let search of searches) {
                let endsWith = search.endsWith('*');
                let startsWith = search.startsWith('*');
                search = search.replace(/\*/g, '');
                let filter;
                if (!endsWith && !startsWith) {
                    filter = ([i, n]) => n.includes(' ' + search + ' ') ? i : null;
                } else if (!endsWith && startsWith) {
                    filter = ([i, n]) => n.includes(search + ' ') ? i : null;
                } else if (endsWith && !startsWith) {
                    filter = ([i, n]) => n.includes(' ' + search) ? i : null;
                } else if (endsWith && startsWith) {
                    filter = ([i, n]) => n.includes(search) ? i : null;
                }
                let idents = pairs.map(filter).filter(Boolean);
                ret.push(idents);
            }
            return ret;
        },
        get cases() {
            return {
                [SELECT_BY_PK]: () => {
                    let ret = [];
                    for (let id of this.ids) {
                        ret.push(this.db[id].ident);
                    }
                    return ret;
                },
                [SELECT_ALL]: () => {
                    let idents = Object.values(this.db).map(({ident: i}) => i);
                    return idents;
                },
                [COUNT_ALL]: async () => {
                    return Object.keys(this.db).length;
                },
                [SELECT_BY_INDEX]: ({countries}) => {
                    let ret = [];
                    let pairs = Object.values(this.db).map(({ident: i, country: c}) => [i, c]);
                    for (let country of countries) {
                        let idents = pairs.map(([i, c]) => c == country ? i : null).filter(Boolean);
                        ret.push(idents);
                    }
                    return ret;
                },
                [COUNT_BY_INDEX]: ({countries}) => {
                    let ret = [];
                    let pairs = Object.values(this.db).map(({ident: i, country: c}) => [i, c]);
                    for (let country of countries) {
                        let idents = pairs.map(([i, c]) => (c == country) ? i : null).filter(Boolean);
                        ret.push(idents.length);
                    }
                    return ret;
                },
                [SELECT_BY_RANGE]: ({seed}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'range');
                    let pairs = Object.values(this.db).map(({ident: i, min}) => [i, min]);
                    for (let i = 1; i <= rangeCounter; i++) {
                        let lo = rand(1, 1E6);
                        let hi = rand(lo, 1E6);
                        let idents = pairs.map(([i, min]) => (lo <= min && min <= hi) ? i : null).filter(Boolean);
                        ret.push(idents);
                    }
                    return ret;
                },
                [COUNT_BY_RANGE]: ({seed}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'range');
                    let pairs = Object.values(this.db).map(({ident: i, min}) => [i, min]);
                    for (let i = 1; i <= rangeCounter; i++) {
                        let lo = rand(1, 1E6);
                        let hi = rand(lo, 1E6);
                        let idents = pairs.map(([i, min]) => (lo <= min && min <= hi) ? i : null).filter(Boolean);
                        ret.push(idents.length);
                    }
                    return ret;
                },
                [SELECT_BY_MULTI]: ({seed, countries}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'multi');
                    let pairs = Object.values(this.db).map(({ident: i, country: c, min, max}) => [i, c, min, max]);
                    for (let i = 1; i <= multipleCounter; i++) {
                        let lo_min = rand(1, 1E6);
                        let hi_min = rand(lo_min, 1E6);
                        let lo_max = rand(1, 1E6);
                        let hi_max = rand(lo_max, 1E6);
                        let country = rand(countries);
                        let idents = pairs.map(([i, c, min, max]) => (lo_min <= min && min <= hi_min && lo_max <= max && max <= hi_max && c == country) ? i : null).filter(Boolean);
                        ret.push(idents);
                    }
                    return ret;
                },
                [COUNT_BY_MULTI]: ({seed, countries}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'multi');
                    let pairs = Object.values(this.db).map(({ident: i, country: c, min, max}) => [i, c, min, max]);
                    for (let i = 1; i <= multipleCounter; i++) {
                        let lo_min = rand(1, 1E6);
                        let hi_min = rand(lo_min, 1E6);
                        let lo_max = rand(1, 1E6);
                        let hi_max = rand(lo_max, 1E6);
                        let country = rand(countries);
                        let idents = pairs.map(([i, c, min, max]) => (lo_min <= min && min <= hi_min && lo_max <= max && max <= hi_max && c == country) ? i : null).filter(Boolean);
                        ret.push(idents.length);
                    }
                    return ret;
                },
                [FULLTEXT]: () => {
                    return this.textSearch(searches.filter((s) => /^[^*].*[^*]$/.test(s)));
                },
                [LOOKAHEAD]: () => {
                    return this.textSearch(searches.filter((s) => /^[^*].*\*$/.test(s)));
                },
                [LOOKBEHIND]: () => {
                    return this.textSearch(searches.filter((s) => /^\*.*[^*]$/.test(s)));
                },
                [SUBSTRING]: () => {
                    return this.textSearch(searches.filter((s) => /^\*.*\*$/.test(s)));
                },
                [RANDOM_UPDATE]: ({seed}) => {
                    let ids = utils.shuffle(this.ids, seed + 'update');
                    let i = 1;
                    for (let id of ids) {
                        this.db[id].ident = 'ID' + (i++);
                    }
                    return [];
                },
                [RANDOM_DELETE]: ({seed}) => {
                    let ids = utils.shuffle(this.ids, seed + 'delete');
                    for (let id of ids) {
                        delete this.db[id];
                    }
                    return [];
                },
            };
        },
    };
}

function getCobaClient() {
    return {
        get name() {
            return 'coba';
        },
        get client() {
            if (!this._client) {
                this._client = coba.makeStorageRpcClient({
                    interf: 'localhost:20000',
                });
            }
            return this._client;
        },
        async connect() {
            return this.client.SetLoggerLevel({level: 'NONE'});
        },
        disconnect() {
            return this.client.end();
        },
        ping() {
            return this.client.isAlive();
        },
        async drop() {
            await this.client.Drop({repository: 'item'});
            this.ids = [];
        },
        async create() {
            this.ids = [];
            await this.client.Create({
                repository: 'item',
                fields: {
                    ident: {type: 'StringIndex', notnull: true},
                    unique_ident: {type: 'Unique', fields: ['ident']},
                    country: {type: 'StringIndex', notnull: false},
                    min: {type: 'NumberIndex', notnull: true, min: 1, max: 1E6},
                    max: {type: 'NumberIndex', notnull: true, min: 1, max: 1E6},
                    name: {type: 'Fulltext', notnull: true},
                },
            });
        },
        async insert(record) {
            let {id} = await this.client.Insert({
                repository: 'item',
                values: {
                    ident: record.ident,
                    country: record.country,
                    min: record.min,
                    max: record.max,
                    name: record.name,
                },
            }).catch(() => ({id: null}));
            if (id != null) {
                this.ids.push(id);
            }
            return id != null;
        },
        async textSearch(searches) {
            let ret = [];
            for (let search of searches) {
                let endsWith = search.endsWith('*');
                let startsWith = search.startsWith('*');
                search = search.replace(/\*/g, '');
                let query;
                if (!endsWith && !startsWith) {
                    query = search;
                } else if (!endsWith && startsWith) {
                    query = '*' + search;
                } else if (endsWith && !startsWith) {
                    query = search + '*';
                } else if (endsWith && startsWith) {
                    query = '*' + search + '*';
                }
                let iterator = await this.client.Iterate({repository: 'item', query, fields: ['ident']});
                let records = await utils.iteratorToArray(iterator.iterate(10000));
                ret.push(records.map(({values}) => values.ident));
            }
            return ret;
        },
        get cases() {
            return {
                [SELECT_BY_PK]: async () => {
                    let ret = [];
                    for (let id of this.ids) {
                        let {records: [record]} = await this.client.Get({repository: 'item', ids: [id], fields: ['ident']});
                        ret.push(record.values.ident);
                    }
                    return ret;
                },
                [SELECT_ALL]: async () => {
                    let iterator = await this.client.Iterate({repository: 'item', fields: ['ident']});
                    let records = await utils.iteratorToArray(iterator.iterate(10000));
                    return records.map(({values}) => values.ident);
                },
                [COUNT_ALL]: async () => {
                    let iterator = await this.client.Iterate({repository: 'item', fields: []});
                    return iterator.count;
                },
                [SELECT_BY_INDEX]: async ({countries}) => {
                    let ret = [];
                    for (let country of countries) {
                        let iterator = await this.client.Iterate({repository: 'item', query: {country}, fields: ['ident']});
                        let records = await utils.iteratorToArray(iterator.iterate(10000));
                        ret.push(records.map(({values}) => values.ident));
                    }
                    return ret;
                },
                [COUNT_BY_INDEX]: async ({countries}) => {
                    let ret = [];
                    for (let country of countries) {
                        let iterator = await this.client.Iterate({repository: 'item', query: {country}, fields: []});
                        ret.push(iterator.count);
                    }
                    return ret;
                },
                [SELECT_BY_RANGE]: async ({seed}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'range');
                    for (let i = 1; i <= rangeCounter; i++) {
                        let lo = rand(1, 1E6);
                        let hi = rand(lo, 1E6);
                        let iterator = await this.client.Iterate({repository: 'item', query: {min: [lo, hi]}, fields: ['ident']});
                        let records = await utils.iteratorToArray(iterator.iterate(10000));
                        ret.push(records.map(({values}) => values.ident));
                    }
                    return ret;
                },
                [COUNT_BY_RANGE]: async ({seed}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'range');
                    for (let i = 1; i <= rangeCounter; i++) {
                        let lo = rand(1, 1E6);
                        let hi = rand(lo, 1E6);
                        let iterator = await this.client.Iterate({repository: 'item', query: {min: [lo, hi]}, fields: ['ident']});
                        ret.push(iterator.count);
                    }
                    return ret;
                },
                [SELECT_BY_MULTI]: async ({seed, countries}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'multi');
                    for (let i = 1; i <= multipleCounter; i++) {
                        let lo_min = rand(1, 1E6);
                        let hi_min = rand(lo_min, 1E6);
                        let lo_max = rand(1, 1E6);
                        let hi_max = rand(lo_max, 1E6);
                        let country = rand(countries);
                        let iterator = await this.client.Iterate({repository: 'item', query: {min: [lo_min, hi_min], max: [lo_max, hi_max], country}, fields: ['ident']});
                        let records = await utils.iteratorToArray(iterator.iterate(10000));
                        ret.push(records.map(({values}) => values.ident));
                    }
                    return ret;
                },
                [COUNT_BY_MULTI]: async ({seed, countries}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'multi');
                    for (let i = 1; i <= multipleCounter; i++) {
                        let lo_min = rand(1, 1E6);
                        let hi_min = rand(lo_min, 1E6);
                        let lo_max = rand(1, 1E6);
                        let hi_max = rand(lo_max, 1E6);
                        let country = rand(countries);
                        let iterator = await this.client.Iterate({repository: 'item', query: {min: [lo_min, hi_min], max: [lo_max, hi_max], country}, fields: ['ident']});
                        ret.push(iterator.count);
                    }
                    return ret;
                },
                [FULLTEXT]: () => {
                    return this.textSearch(searches.filter((s) => /^[^*].*[^*]$/.test(s)));
                },
                [LOOKAHEAD]: () => {
                    return this.textSearch(searches.filter((s) => /^[^*].*\*$/.test(s)));
                },
                [LOOKBEHIND]: () => {
                    return this.textSearch(searches.filter((s) => /^\*.*[^*]$/.test(s)));
                },
                [SUBSTRING]: async () => {
                    return this.textSearch(searches.filter((s) => /^\*.*\*$/.test(s)));
                },
                [RANDOM_UPDATE]: async ({seed}) => {
                    let ids = utils.shuffle(this.ids, seed + 'update');
                    let i = 1;
                    for (let id of ids) {
                        await this.client.Update({repository: 'item', id, values: {ident: 'ID' + (i++)}});
                    }
                    return [];
                },
                [RANDOM_DELETE]: async ({seed}) => {
                    let ids = utils.shuffle(this.ids, seed + 'delete');
                    for (let id of ids) {
                        await this.client.Delete({repository: 'item', id});
                    }
                    return [];
                },
            };
        },
    };
}

function getPostgresClient() {
    return {
        get name() {
            return 'postgres';
        },
        get client() {
            if (!this._client) {
                this._client = new Client({
                    user: 'postgres',
                    host: 'localhost',
                    database: 'postgres',
                    password: 'password',
                    port: 5432,
                });
            }
            return this._client;
        },
        connect() {
            return this.client.connect();
        },
        disconnect() {
            this.client.end();
        },
        async ping() {
            let res = await this.client.query('SELECT NOW()');
            return Array.isArray(res.rows);
        },
        async create() {
            this.ids = [];
            let queries = [
                `
                    CREATE TABLE item (
                        item_id serial PRIMARY KEY,
                        ident VARCHAR(32) NOT NULL,
                        country VARCHAR(32) NULL,
                        min INTEGER NOT NULL,
                        max INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        name_ts TSVECTOR NOT NULL
                    );
                `,
                `CREATE UNIQUE INDEX ident_idx ON item (ident);`,
                `CREATE INDEX country_idx ON item (country);`,
            ];
            for (let query of queries) {
                await this.client.query(query);
            }
        },
        async drop() {
            await this.client.query('DROP TABLE IF EXISTS item');
            this.ids = [];
        },
        async insert(record) {
            let {rows: [{id}]} = await this.client.query(`
                INSERT INTO item (ident, country, min, max, name, name_ts)
                VALUES ($1, $2, $3, $4, $5, to_tsvector($5))
                RETURNING item_id AS id
            `, [
                record.ident,
                record.country,
                record.min,
                record.max,
                record.name,
            ]).catch(() => ({rows: [{id: null}]}));
            if (id != null) {
                this.ids.push(id);
            }
            return id != null;
        },
        async textSearch(searches) {
            let ret = [];
            for (let search of searches) {
                let endsWith = search.endsWith('*');
                let startsWith = search.startsWith('*');
                search = search.replace(/\*/g, '');
                let args = [];
                if (!endsWith && !startsWith) {
                    args = ['SELECT ident FROM item WHERE name_ts @@ to_tsquery($1)', [search]];
                } else if (!endsWith && startsWith) {
                    args = ['SELECT ident FROM item WHERE name ILIKE $1 OR name ILIKE $2', [`%${search}`, `%${search} %`]];
                } else if (endsWith && !startsWith) {
                    args = ['SELECT ident FROM item WHERE name_ts @@ to_tsquery($1)', [`${search}:*`]];
                } else if (endsWith && startsWith) {
                    args = ['SELECT ident FROM item WHERE name ILIKE $1', [`%${search}%`]];
                }
                let {rows} = await this.client.query(...args);
                ret.push(rows.map((row) => row.ident));
            }
            return ret;
        },
        get cases() {
            return {
                [SELECT_BY_PK]: async () => {
                    let ret = [];
                    for (let id of this.ids) {
                        let {rows: [{ident}]} = await this.client.query('SELECT ident FROM item WHERE item_id = $1', [id]);
                        ret.push(ident);
                    }
                    return ret;
                },
                [SELECT_ALL]: async () => {
                    let {rows} = await this.client.query('SELECT ident FROM item');
                    return rows.map((row) => row.ident);
                },
                [COUNT_ALL]: async () => {
                    let {rows: [row]} = await this.client.query('SELECT COUNT(item_id) AS "count" FROM item');
                    return +row.count;
                },
                [SELECT_BY_INDEX]: async ({countries}) => {
                    let ret = [];
                    for (let country of countries) {
                        let {rows} = await this.client.query('SELECT ident FROM item WHERE country = $1', [country]);
                        ret.push(rows.map((row) => row.ident));
                    }
                    return ret;
                },
                [COUNT_BY_INDEX]: async ({countries}) => {
                    let ret = [];
                    for (let country of countries) {
                        let {rows: [row]} = await this.client.query('SELECT COUNT(item_id) AS "count" FROM item WHERE country = $1', [country]);
                        ret.push(+row.count);
                    }
                    return ret;
                },
                [SELECT_BY_RANGE]: async ({seed}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'range');
                    for (let i = 1; i <= rangeCounter; i++) {
                        let lo = rand(1, 1E6);
                        let hi = rand(lo, 1E6);
                        let {rows} = await this.client.query('SELECT ident FROM item WHERE $1 <= min AND min <= $2', [lo, hi]);
                        ret.push(rows.map((row) => row.ident));
                    }
                    return ret;
                },
                [COUNT_BY_RANGE]: async ({seed}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'range');
                    for (let i = 1; i <= rangeCounter; i++) {
                        let lo = rand(1, 1E6);
                        let hi = rand(lo, 1E6);
                        let {rows: [row]} = await this.client.query('SELECT COUNT(item_id) AS "count" FROM item WHERE $1 <= min AND min <= $2', [lo, hi]);
                        ret.push(+row.count);
                    }
                    return ret;
                },
                [SELECT_BY_MULTI]: async ({seed, countries}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'multi');
                    for (let i = 1; i <= multipleCounter; i++) {
                        let lo_min = rand(1, 1E6);
                        let hi_min = rand(lo_min, 1E6);
                        let lo_max = rand(1, 1E6);
                        let hi_max = rand(lo_max, 1E6);
                        let country = rand(countries);
                        let {rows} = await this.client.query('SELECT ident FROM item WHERE $1 <= min AND min <= $2 AND $3 <= max AND max <= $4 AND country = $5', [lo_min, hi_min, lo_max, hi_max, country]);
                        ret.push(rows.map((row) => row.ident));
                    }
                    return ret;
                },
                [COUNT_BY_MULTI]: async ({seed, countries}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'multi');
                    for (let i = 1; i <= multipleCounter; i++) {
                        let lo_min = rand(1, 1E6);
                        let hi_min = rand(lo_min, 1E6);
                        let lo_max = rand(1, 1E6);
                        let hi_max = rand(lo_max, 1E6);
                        let country = rand(countries);
                        let {rows: [row]} = await this.client.query('SELECT COUNT(item_id) AS "count" FROM item WHERE $1 <= min AND min <= $2 AND $3 <= max AND max <= $4 AND country = $5', [lo_min, hi_min, lo_max, hi_max, country]);
                        ret.push(+row.count);
                    }
                    return ret;
                },
                [FULLTEXT]: () => {
                    return this.textSearch(searches.filter((s) => /^[^*].*[^*]$/.test(s)));
                },
                [LOOKAHEAD]: () => {
                    return this.textSearch(searches.filter((s) => /^[^*].*\*$/.test(s)));
                },
                [LOOKBEHIND]: () => {
                    return this.textSearch(searches.filter((s) => /^\*.*[^*]$/.test(s)));
                },
                [SUBSTRING]: async () => {
                    return this.textSearch(searches.filter((s) => /^\*.*\*$/.test(s)));
                },
                [RANDOM_UPDATE]: async ({seed}) => {
                    let ids = utils.shuffle(this.ids, seed + 'update');
                    let i = 1;
                    for (let id of ids) {
                        await this.client.query('UPDATE item SET ident = $1 WHERE item_id = $2', ['ID' + (i++), id]);
                    }
                    return [];
                },
                [RANDOM_DELETE]: async ({seed}) => {
                    let ids = utils.shuffle(this.ids, seed + 'delete');
                    for (let id of ids) {
                        await this.client.query('DELETE FROM item WHERE item_id = $1', [id]);
                    }
                    return [];
                },
            };
        },
    };
}

function getRedisearchClient() {
    return {
        get name() {
            return 'redisearch';
        },
        get client() {
            if (!this._client) {
                this._client = new Redis({
                    host: '127.0.0.1',
                    port: 6379,
                });
            }
            return this._client;
        },
        connect() {
        },
        disconnect() {
            return this.client.disconnect();
        },
        async ping() {
            return /PONG/.test(await this.client.ping());
        },
        async drop() {
            await this.client.call('FT.DROP', 'item').catch(NOOP);
            this.ids = [];
        },
        async create() {
            this.ids = [];
            await this.client.call(
                'FT.CREATE', 'item', 'SCHEMA',
                'ident', 'TAG',
                'country', 'TAG',
                'min', 'NUMERIC',
                'max', 'NUMERIC',
                'name', 'TEXT',
            );
        },
        async insert(record) {
            let id = this.ids.length + 1;
            let [found] = await this.client.call(
                'FT.SEARCH', 'item', `@ident:{${record.ident}}`,
                'LIMIT', 0, 0,
            );
            if (found) {
                return;
            }
            await this.client.call(
                'FT.ADD', 'item', id, '1.0',
                'FIELDS',
                'ident', record.ident,
                'min', record.min,
                'max', record.max,
                'name', record.name,
                ...[record.country != null ? ['country', record.country] : []],
            );
            this.ids.push(id);
            return true;
        },
        async textSearch(searches) {
            let ret = [];
            for (let search of searches) {
                let endsWith = search.endsWith('*');
                let startsWith = search.startsWith('*');
                search = search.replace(/\*/g, '');
                let query;
                if (!endsWith && !startsWith) {
                    query = search;
                } else if (!endsWith && startsWith) {
                    query = '*' + search;
                } else if (endsWith && !startsWith) {
                    query = search + '*';
                } else if (endsWith && startsWith) {
                    query = '*' + search + '*';
                }
                let res = await this.client.call(
                    'FT.SEARCH', 'item',
                    query,
                    'RETURN', '1', 'ident',
                    'LIMIT', '0', '1000000',
                );
                res.shift();
                res = utils.chunk(res, 2);
                ret.push(res.map((r) => r.pop().pop()));
            }
            return ret;
        },
        get cases() {
            return {
                [SELECT_BY_PK]: async () => {
                    let ret = [];
                    for (let id of this.ids) {
                        let [, ident] = await this.client.call(
                            'FT.GET', 'item', id,
                        );
                        ret.push(ident);
                    }
                    return ret;
                },
                [SELECT_ALL]: async () => {
                    let res = await this.client.call(
                        'FT.SEARCH', 'item', '*',
                        'LIMIT', '0', '1000000',
                        'RETURN', '1', 'ident',
                        'LIMIT', '0', '1000000',
                    );
                    res.shift();
                    res = utils.chunk(res, 2);
                    return res.map((r) => r.pop().pop());
                },
                [COUNT_ALL]: async () => {
                    let res = await this.client.call(
                        'FT.SEARCH', 'item', '*',
                        'LIMIT', '0', '0',
                    );
                    return res.shift();
                },
                [SELECT_BY_INDEX]: async ({countries}) => {
                    let ret = [];
                    for (let country of countries) {
                        let res = await this.client.call(
                            'FT.SEARCH', 'item',
                            `@country:{${country}}`,
                            'RETURN', '1', 'ident',
                            'LIMIT', '0', '1000000',
                        );
                        res.shift();
                        res = utils.chunk(res, 2);
                        ret.push(res.map((r) => r.pop().pop()));
                    }
                    return ret;
                },
                [COUNT_BY_INDEX]: async ({countries}) => {
                    let ret = [];
                    for (let country of countries) {
                        let res = await this.client.call(
                            'FT.SEARCH', 'item',
                            `@country:{${country}}`,
                            'LIMIT', '0', '0',
                        );
                        ret.push(res.shift());
                    }
                    return ret;
                },
                [SELECT_BY_RANGE]: async ({seed}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'range');
                    for (let i = 1; i <= rangeCounter; i++) {
                        let lo = rand(1, 1E6);
                        let hi = rand(lo, 1E6);
                        let res = await this.client.call(
                            'FT.SEARCH', 'item',
                            `@min:[${lo} ${hi}]`,
                            'RETURN', '1', 'ident',
                            'LIMIT', '0', '1000000',
                        );
                        res.shift();
                        res = utils.chunk(res, 2);
                        ret.push(res.map((r) => r.pop().pop()));
                    }
                    return ret;
                },
                [COUNT_BY_RANGE]: async ({seed}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'range');
                    for (let i = 1; i <= rangeCounter; i++) {
                        let lo = rand(1, 1E6);
                        let hi = rand(lo, 1E6);
                        let res = await this.client.call(
                            'FT.SEARCH', 'item',
                            `@min:[${lo} ${hi}]`,
                            'LIMIT', '0', '0',
                        );
                        ret.push(res.shift());
                    }
                    return ret;
                },
                [SELECT_BY_MULTI]: async ({seed, countries}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'multi');
                    for (let i = 1; i <= multipleCounter; i++) {
                        let lo_min = rand(1, 1E6);
                        let hi_min = rand(lo_min, 1E6);
                        let lo_max = rand(1, 1E6);
                        let hi_max = rand(lo_max, 1E6);
                        let country = rand(countries);
                        let res = await this.client.call(
                            'FT.SEARCH', 'item',
                            `@min:[${lo_min} ${hi_min}] @max:[${lo_max} ${hi_max}] @country:{${country}}`,
                            'RETURN', '1', 'ident',
                            'LIMIT', '0', '1000000',
                        );
                        res.shift();
                        res = utils.chunk(res, 2);
                        ret.push(res.map((r) => r.pop().pop()));
                    }
                    return ret;
                },
                [COUNT_BY_MULTI]: async ({seed, countries}) => {
                    let ret = [];
                    let rand = utils.srand(seed + 'multi');
                    for (let i = 1; i <= multipleCounter; i++) {
                        let lo_min = rand(1, 1E6);
                        let hi_min = rand(lo_min, 1E6);
                        let lo_max = rand(1, 1E6);
                        let hi_max = rand(lo_max, 1E6);
                        let country = rand(countries);
                        let res = await this.client.call(
                            'FT.SEARCH', 'item',
                            `@min:[${lo_min} ${hi_min}] @max:[${lo_max} ${hi_max}] @country:{${country}}`,
                            'LIMIT', '0', '0',
                        );
                        ret.push(res.shift());
                    }
                    return ret;
                },
                [FULLTEXT]: () => {
                    return this.textSearch(searches.filter((s) => /^[^*].*[^*]$/.test(s)));
                },
                [LOOKAHEAD]: () => {
                    return this.textSearch(searches.filter((s) => /^[^*].*\*$/.test(s)));
                },
                [RANDOM_DELETE]: async ({seed}) => {
                    let ids = utils.shuffle(this.ids, seed + 'delete');
                    for (let id of ids) {
                        await this.client.call('FT.DEL', 'item', id, 'DD');
                    }
                    return [];
                },
            };
        },
    };
}

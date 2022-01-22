const utils = require('ejz-utils');

const {
    StorageRpcClient,
    makeStorageRpcServer,
    makeStorageRpcClient,
    onShutdown,
} = require('./di');

afterEach(onShutdown);

function getRpcClient(options) {
    return getRpcServerClient(options).then((r) => r.client);
}

async function getRpcServerClient(options) {
    options = options ?? {};
    options.interf = options.interf ?? utils.getRandomInterface();
    let client = makeStorageRpcClient(options);
    let server = await makeStorageRpcServer(options);
    return {server, client};
}

test('StorageRpc / isAlive', async () => {
    let c = await getRpcClient();
    expect(await c.isAlive()).toEqual(true);
});

test('StorageRpc / List, Create, Drop, Exists', async () => {
    let c = await getRpcClient();
    expect(await c.List()).toEqual({repositories: []});
    expect(await c.Create({repository: 'r'})).toEqual({created: true});
    expect(await c.Create({repository: 'r'})).toEqual({created: false});
    expect(await c.List()).toEqual({repositories: ['r']});
    expect(await c.Exists({repository: 'r'})).toEqual({exists: true});
    expect(await c.Drop({repository: 'r'})).toEqual({dropped: true});
    expect(await c.Exists({repository: 'r'})).toEqual({exists: false});
    expect(await c.Drop({repository: 'r'})).toEqual({dropped: false});
    expect(await c.List()).toEqual({repositories: []});
});

test('StorageRpc / Insert, Get, Has, Delete', async () => {
    let c = await getRpcClient();
    let res1 = await c.Insert().catch((e) => e);
    expect(res1 instanceof Error).toEqual(true);
    let res2 = await c.Insert({repository: 'r'}).catch((e) => e);
    expect(res2 instanceof Error).toEqual(true);
    await c.Create({repository: 'r'});
    let {id: id1} = await c.Insert({repository: 'r'});
    let {id: id2} = await c.Insert({repository: 'r'});
    expect(id1 == 1 && id2 == 2).toEqual(true);
    let res3 = await c.Get({repository: 'r', ids: [2, 100, 1]});
    expect(res3.records).toEqual([{id: 2, values: {}}, {id: 1, values: {}}]);
    let res4 = await c.Has({repository: 'r', id: 2});
    expect(res4.has).toEqual(true);
    let res5 = await c.Has({repository: 'r', id: 100});
    expect(res5.has).toEqual(false);
    let res6 = await c.Delete({repository: 'r', id: 1});
    expect(res6.deleted).toEqual(true);
    let res7 = await c.Delete({repository: 'r', id: 1});
    expect(res7.deleted).toEqual(false);
    let res8 = await c.Has({repository: 'r', id: 1});
    expect(res8.has).toEqual(false);
});

test('StorageRpc / Iterate, Next', async () => {
    let c = await getRpcClient();
    await c.Create({repository: 'r'});
    for (let i = 1; i <= 1000; i++) {
        await c.Insert({repository: 'r'});
    }
    let res1 = await c.Iterate({repository: 'r'});
    expect(!!res1.iterator).toEqual(true);
    expect(res1.count).toEqual(1000);
    expect(res1.miss).toEqual(0);
    let res2 = await c.Next({repository: 'r', limit: 2, iterator: res1.iterator});
    res2.records = res2.records.map((rec) => rec.id);
    expect(res2).toEqual({records: [1, 2], offset: 0, done: false});
    let res3 = await c.Next({repository: 'r', limit: 0, iterator: res1.iterator});
    expect(res3).toEqual({records: [], offset: 2, done: false});
    let res4 = await c.Next({repository: 'r', limit: 998, iterator: res1.iterator});
    res4.records = res4.records.map((rec) => rec.id);
    expect(res4).toEqual({records: new Array(998).fill(null).map((_, i) => i + 3), offset: 2, done: false});
    let res5 = await c.Next({repository: 'r', limit: 100, iterator: res1.iterator});
    expect(res5).toEqual({records: [], offset: 1000, done: true});
    let res6 = await c.Next({repository: 'r', limit: 1, iterator: res1.iterator}).catch((e) => e);
    expect(res6 instanceof Error).toEqual(true);
    let res7 = await c.Iterate({repository: 'r'});
    let id1 = 1;
    for await (let record of res7) {
        expect(record).toEqual({id: id1++, values: {}});
    }
    let res8 = await c.Iterate({repository: 'r'});
    let id2 = 1;
    for await (let records of res8.iterate(3, true)) {
        if (records.length == 1) {
            expect(records).toEqual([
                {id: id2++, values: {}},
            ]);
        } else {
            expect(records).toEqual([
                {id: id2++, values: {}},
                {id: id2++, values: {}},
                {id: id2++, values: {}},
            ]);
        }
    }
    let res9 = await c.Iterate({repository: 'r', miss: 10});
    let count = 0;
    for await (let _ of res9) {
        count++;
    }
    expect(Math.abs(count - 900) < 100).toEqual(true);
});

test('StorageRpc / Fields', async () => {
    let c = await getRpcClient();
    let cases = [
        [{type: 'String'}, {type: 'String', notnull: false}],
        [{type: 'String', notnull: true}, {type: 'String', notnull: true}],
        [{type: 'Number', min: '-1E3', max: '20'}, {type: 'Number', notnull: false, precision: 0, min: '-1E3', max: '20'}],
        [{type: 'Unique', fields: ['b']}, {type: 'Unique', fields: ['b']}],
    ];
    for (let [one, two] of cases) {
        let repository = 'r' + Number(new Date());
        await c.Create({repository, fields: {a: one, b: {type: 'StringIndex'}}});
        let res = await c.Fields({repository});
        delete res.fields.b;
        expect(res.fields).toEqual({a: two});
    }
});

test('StorageRpc / values', async () => {
    let c = await getRpcClient();
    let repository = 'r';
    await c.Create({repository, fields: {
        string: {type: 'String'},
        bool: {type: 'Boolean'},
        number: {type: 'Number'},
    }});
    let {id} = await c.Insert({repository, values: {string: 's', bool: true, number: 5}});
    let {records: [rec]} = await c.Get({repository, ids: [id], fields: ['string', 'bool', 'number']});
    expect(rec.values).toEqual({string: 's', bool: true, number: 5});
    let iterator = await c.Iterate({repository, fields: ['string', 'bool', 'number']});
    let _record;
    for await (let record of iterator) {
        _record = record;
    }
    expect(_record.values).toEqual({string: 's', bool: true, number: 5});
});

test('StorageRpc / Update', async () => {
    let c = await getRpcClient();
    let repository = 'r';
    await c.Create({repository, fields: {string: {type: 'String'}}});
    let {id} = await c.Insert({repository, values: {string: 's'}});
    let res1 = await c.Update().catch((e) => e);
    expect(res1 instanceof Error).toEqual(true);
    let res2 = await c.Update({repository}).catch((e) => e);
    expect(res2 instanceof Error).toEqual(true);
    let res3 = await c.Update({repository, id: 100}).catch((e) => e);
    expect(res3 instanceof Error).toEqual(true);
    let {records: [rec1]} = await c.Get({repository, ids: [id], fields: ['string']});
    expect(rec1.values).toEqual({string: 's'});
    let res4 = await c.Update({repository, id, values: {s: '_'}}).catch((e) => e);
    expect(res4 instanceof Error).toEqual(true);
    let res5 = await c.Update({repository, id, values: {string: '_'}});
    expect(res5).toEqual({updated: true});
    let {records: [rec2]} = await c.Get({repository, ids: [id], fields: ['string']});
    expect(rec2.values).toEqual({string: '_'});
});

test('StorageRpc / Lock', async () => {
    let c = await getRpcClient();
    let repository = 'r';
    await c.Create({repository});
    let {repositories} = await c.List();
    expect(repositories).toEqual(['r']);
    let res1 = await c.Lock();
    expect(res1).toEqual({locked: true});
    let res2 = await c.Lock();
    expect(res2).toEqual({locked: false});
    let res3 = await c.List().catch((e) => e);
    expect(res3 instanceof Error).toEqual(true);
    let res4 = await c.Sync();
    expect(res4).toEqual({synced: true});
});

test('StorageRpc / LoggerLevel', async () => {
    let c = await getRpcClient();
    await c.SetLoggerLevel({}).catch(String);
    let {level} = await c.GetLoggerLevel();
    expect(!!level).toEqual(true);
});

test('StorageRpc / Unique Fields', async () => {
    let repository = 'r';
    let c = await getRpcClient();
    let {created} = await c.Create({
        repository,
        fields: {
            unique: {
                type: 'Unique',
                fields: ['string'],
            },
            string: {
                type: 'StringIndex',
            },
        },
    });
    expect(created).toEqual(true);
});

test('StorageRpc / Constructor', async () => {
    class MyRpcClient extends StorageRpcClient {
        IteratePostProcess(res) {
            let iterator = super.IteratePostProcess(res);
            let _iterate = iterator.iterate.bind(iterator);
            iterator.iterate = (limit, chunk) => {
                limit = limit ?? 2;
                chunk = chunk ?? true;
                return utils.iteratorMap(_iterate(limit, chunk), (rec) => {
                    rec.toString = () => 'REC:' + rec.id;
                    return rec;
                });
            };
            return iterator;
        }
    }
    let c = await getRpcClient({_constructor: MyRpcClient});
    expect(await c.isAlive()).toEqual(true);
    await c.Create({repository: 'r'});
    for (let i = 1; i <= 1000; i++) {
        await c.Insert({repository: 'r'});
    }
    let res1 = await c.Iterate({repository: 'r'});
    expect(!!res1.iterator).toEqual(true);
    expect(res1.count).toEqual(1000);
    let all1 = await utils.iteratorToArray(res1);
    expect(all1.length).toEqual(500);
    let res2 = await c.Iterate({repository: 'r'});
    let all2 = await utils.iteratorToArray(res2.iterate(10, false));
    expect(all2.length).toEqual(1000);
    for (let rec of all2) {
        expect(String(rec)).toMatch(/^REC:\d+$/);
    }
});

test('StorageRpc / QueryParserError', async () => {
    let c = await getRpcClient();
    expect(await c.isAlive()).toEqual(true);
    await c.Create({repository: 'r'});
    let err = await c.Iterate({repository: 'r', query: '****'}).catch(String);
    expect(err).toMatch(/RepositoryQueryError/);
    expect(err).toMatch(/QueryParserInvalidSyntaxError/);
    expect(err).toMatch('****');
});

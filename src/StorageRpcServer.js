const os = require('os');
const utils = require('ejz-utils');

const {Storage} = require('./Storage');
const {ITERATOR_TIMEOUT} = require('./constants');
const {RepositoryError} = require('./Repository');

class StorageRpcServer {
    constructor(options) {
        options = options ?? {};
        let {logger, storage, interf, v8Serializer, objectShallowCopy} = options;
        utils.ok.instance(utils.Logger, logger);
        utils.ok.instance(Storage, storage);
        this.logger = logger;
        this.storage = storage;
        this.promise = utils.getRpcServer({
            interf,
            logger: this.logger,
            onRpc: this.onRpc.bind(this),
            v8Serializer,
            objectShallowCopy,
        }).then((server) => {
            this.server = server;
            delete this.promise;
            return this;
        });
        this.request_id = 1;
        this.iterator_id = 1;
        this.iterators = new utils.Debounced({ms: ITERATOR_TIMEOUT});
        this.locked = false;
        this.systemCalls = ['Stat', 'Lock', 'Unlock', 'Ping', 'SetLoggerLevel', 'GetLoggerLevel', 'Sync'];
    }

    async close() {
        if (this.promise) {
            await this.promise;
            return this.close();
        }
        return this.server.close();
    }

    checkArgument(args, name) {
        let arg = args?.[0]?.[name];
        if (arg == null) {
            throw new StorageRpcServerInvalidArgumentError(name);
        }
        return arg;
    }

    checkRepository(args) {
        let repository = this.checkArgument(args, 'repository');
        if (!this.storage.exists(repository)) {
            throw new StorageRpcServerRepositoryNotFoundError(repository);
        }
        args[0].repository = this.storage.getRepository(repository);
        return args;
    }

    checkIterator(args) {
        let iterator = this.checkArgument(args, 'iterator');
        iterator = this.iterators.get(iterator);
        if (!iterator) {
            throw new StorageRpcServerIteratorNotFoundError(iterator);
        }
        if (iterator.done) {
            throw new StorageRpcServerIteratorExhaustedError(iterator);
        }
        args[0].iterator = iterator;
        return args;
    }

    CreatePreProcess(args) {
        this.checkArgument(args, 'repository');
        return args;
    }

    DropPreProcess(args) {
        this.checkArgument(args, 'repository');
        return args;
    }

    ExistsPreProcess(args) {
        this.checkArgument(args, 'repository');
        return args;
    }

    InsertPreProcess(args) {
        return this.checkRepository(args);
    }

    GetPreProcess(args) {
        return this.checkRepository(args);
    }

    IteratePreProcess(args) {
        return this.checkRepository(args);
    }

    FieldsPreProcess(args) {
        return this.checkRepository(args);
    }

    UpdatePreProcess(args) {
        this.checkArgument(args, 'id');
        return this.checkRepository(args);
    }

    HasPreProcess(args) {
        this.checkArgument(args, 'id');
        return this.checkRepository(args);
    }

    DeletePreProcess(args) {
        this.checkArgument(args, 'id');
        return this.checkRepository(args);
    }

    NextPreProcess(args) {
        this.checkArgument(args, 'limit');
        return this.checkIterator(args);
    }

    SetLoggerLevelPreProcess(args) {
        let level = this.checkArgument(args, 'level');
        if (!utils.Logger.levels.includes(level)) {
            throw new StorageRpcServerInvalidArgumentError('level');
        }
        return args;
    }

    async onRpc(call, ...args) {
        let rid = '(' + (this.request_id++) + ')';
        try {
            if (!call || !this[call]) {
                throw new StorageRpcServerInvalidCallError();
            }
            this.logger.dbg('->', rid, call, ...args);
            if (this.locked && !this.systemCalls.includes(call)) {
                throw new StorageRpcServerLockError();
            }
            let [pre, post] = [`${call}PreProcess`, `${call}PostProcess`];
            if (this[pre]) {
                args = await this[pre](args);
            }
            let resp = await this[call](...args);
            if (this[post]) {
                resp = await this[post](resp, args);
            }
            this.logger.dbg('<-', rid, resp);
            return resp;
        } catch (error) {
            let _error = utils.stringifyError(error, true);
            this.logger.err('--', rid, (error instanceof RepositoryError) ? _error : error);
            return {error: _error};
        }
    }

    async Sync() {
        await this.sync();
        return {synced: true};
    }

    Ping() {
        return {pinged: true};
    }

    List() {
        let repositories = this.storage.list();
        return {repositories};
    }

    Create(req) {
        let created = this.storage.create(req.repository, req.fields);
        return {created};
    }

    Drop(req) {
        let dropped = this.storage.drop(req.repository);
        return {dropped};
    }

    Exists(req) {
        let exists = this.storage.exists(req.repository);
        return {exists};
    }

    Insert(req) {
        let repository = req.repository;
        let values = req.values;
        let id = repository.insert(values);
        return {id};
    }

    Update(req) {
        let repository = req.repository;
        let id = req.id;
        let values = req.values;
        repository.update(id, values);
        return {updated: true};
    }

    Get(req) {
        let {ids, fields, repository} = req;
        ids = ids ?? [];
        let records = [];
        for (let id of ids) {
            let values = repository.get(id, fields);
            if (values) {
                records.push({id, values});
            }
        }
        return {records};
    }

    Has(req) {
        let repository = req.repository;
        let id = req.id;
        let has = repository.has(id);
        return {has};
    }

    Delete(req) {
        let repository = req.repository;
        let id = req.id;
        let deleted = repository.delete(id);
        return {deleted};
    }

    Iterate(req) {
        let repository = req.repository;
        let iterator = repository.iterate(
            (req.query ?? null),
            (req.fields ?? []),
            (req.sort ?? null),
            (req.asc ?? null),
            (req.miss ?? null),
            (req.random ?? null),
        );
        let iterator_id = this.iterator_id++;
        this.iterators.set(iterator_id, iterator);
        return {
            iterator: iterator_id,
            count: iterator.count,
            miss: iterator.miss,
        };
    }

    Next(req) {
        let iterator = req.iterator;
        let limit = req.limit;
        let offset = iterator.offset;
        let records = [];
        while (limit-- > 0) {
            let {done, value} = iterator.next();
            if (done) {
                break;
            }
            let [id, values] = value;
            records.push({id, values});
        }
        return {records, offset, done: iterator.done};
    }

    Fields(req) {
        let repository = req.repository;
        return {fields: repository.getFieldOptions()};
    }

    async sync() {
        await this.iterators.sync();
        await this.storage.sync();
    }

    Lock() {
        let ex = this.locked;
        this.locked = true;
        return {locked: ex != this.locked};
    }

    Unlock() {
        let ex = this.locked;
        this.locked = false;
        return {unlocked: ex != this.locked};
    }

    Stat() {
        let memoryUsage = process.memoryUsage();
        let load_average = os.loadavg().shift();
        let cpus = os.cpus().length;
        return {
            cpus,
            load_average,
            memory_rss: memoryUsage.rss,
            memory_heap_total: memoryUsage.heapTotal,
            memory_heap_used: memoryUsage.heapUsed,
            memory_external: memoryUsage.external,
            memory_array_buffers: memoryUsage.arrayBuffers || 0,
        };
    }

    GetLoggerLevel() {
        let {level} = this.logger;
        return {level};
    }

    SetLoggerLevel(req) {
        let ex = this.logger.level;
        this.logger.level = req.level;
        return {'set': ex != req.level};
    }
}

exports.StorageRpcServer = StorageRpcServer;

class StorageRpcServerError extends Error {}

exports.StorageRpcServerError = StorageRpcServerError;

class StorageRpcServerInvalidArgumentError extends StorageRpcServerError {}

exports.StorageRpcServerInvalidArgumentError = StorageRpcServerInvalidArgumentError;

class StorageRpcServerRepositoryNotFoundError extends StorageRpcServerError {}

exports.StorageRpcServerRepositoryNotFoundError = StorageRpcServerRepositoryNotFoundError;

class StorageRpcServerIteratorNotFoundError extends StorageRpcServerError {}

exports.StorageRpcServerIteratorNotFoundError = StorageRpcServerIteratorNotFoundError;

class StorageRpcServerIteratorExhaustedError extends StorageRpcServerError {}

exports.StorageRpcServerIteratorExhaustedError = StorageRpcServerIteratorExhaustedError;

class StorageRpcServerInvalidCallError extends StorageRpcServerError {}

exports.StorageRpcServerInvalidCallError = StorageRpcServerInvalidCallError;

class StorageRpcServerLockError extends StorageRpcServerError {}

exports.StorageRpcServerLockError = StorageRpcServerLockError;

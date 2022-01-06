const utils = require('ejz-utils');

const {
    AbstractService,
    AbstractServiceServer,
    AbstractServiceClient,
    AbstractServiceInvalidArgumentError,
    AbstractServiceNotFoundError,
    AbstractServiceResourceExhaustedError,
    AbstractServiceInternalError,
} = require('ejz-grpc');

const {Storage} = require('./Storage');
const {ITERATOR_TIMEOUT} = require('./constants');

const {
    RepositoryError,
    RepositoryInvalidFieldError,
    RepositoryIdNotExistsError,
} = require('./Repository');

class StorageService extends AbstractService {
    constructor(logger, storage) {
        super(logger);
        utils.ok.instance(Storage, storage);
        this.storage = storage;
        this.initService(Object.getOwnPropertyNames(StorageService.prototype), ['Sync']);
        this.iterator_id = 1;
        this.iterators = new utils.Debounced({ms: ITERATOR_TIMEOUT});
    }

    get annotations() {
        return {
            ...super.annotations,
            Create: {
                required: ['repository'],
            },
            Drop: {
                required: ['repository'],
            },
            Exists: {
                required: ['repository'],
            },
            Insert: {
                toRepository: [],
                deserializeValues: [false],
            },
            Update: {
                required: ['id'],
                toRepository: [],
                deserializeValues: [true],
            },
            Get: {
                toRepository: [],
            },
            Has: {
                required: ['id'],
                toRepository: [],
            },
            Delete: {
                required: ['id'],
                toRepository: [],
            },
            Iterate: {
                toRepository: [],
            },
            Next: {
                required: ['limit'],
                toIterator: [],
            },
            Fields: {
                toRepository: [],
            },
        };
    }

    async ServiceList() {
        let repositories = this.storage.list();
        return {repositories};
    }

    async ServiceCreate(req) {
        let fields = req.fields ?? {};
        let created = this.storage.create(req.repository, fields);
        return {created};
    }

    async ServiceDrop(req) {
        let dropped = this.storage.drop(req.repository);
        return {dropped};
    }

    async ServiceExists(req) {
        let exists = this.storage.list().includes(req.repository);
        return {exists};
    }

    async ServiceInsert(req) {
        let repository = req.repository;
        let values = req.values;
        let id = repository.insert(values);
        return {id};
    }

    async ServiceUpdate(req) {
        let repository = req.repository;
        let id = req.id;
        let values = req.values;
        repository.update(id, values);
        return {updated: true};
    }

    async ServiceGet(req) {
        let repository = req.repository;
        let ids = req.ids ?? [];
        let fields = req.fields ?? [];
        let records = [];
        for (let id of ids) {
            let values = repository.get(id, fields);
            if (values) {
                values = utils.remap(values, (k, v) => v != null ? [k, utils.toJson(v)] : null);
                records.push({id, values});
            }
        }
        return {records};
    }

    async ServiceHas(req) {
        let repository = req.repository;
        let id = req.id;
        let has = repository.has(id);
        return {has};
    }

    async ServiceDelete(req) {
        let repository = req.repository;
        let id = req.id;
        let deleted = repository.delete(id);
        return {deleted};
    }

    async ServiceIterate(req) {
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

    async ServiceNext(req) {
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
            values = utils.remap(values ?? {}, (k, v) => v != null ? [k, utils.toJson(v)] : null);
            records.push({id, values});
        }
        return {records, offset, done: iterator.done};
    }

    async ServiceFields(req) {
        let repository = req.repository;
        return {fields: repository.getFieldOptions()};
    }

    async ServiceSync() {
        this.sync();
        return {synced: true};
    }

    sync() {
        this.iterators.sync();
        this.storage.sync();
    }

    toRepository(req) {
        if (req.repository == null) {
            throw new AbstractServiceInvalidArgumentError('repository');
        }
        let exists = this.storage.list().includes(req.repository);
        if (!exists) {
            throw new AbstractServiceNotFoundError('repository.' + req.repository);
        }
        req.repository = this.storage.getRepository(req.repository);
    }

    toIterator(req) {
        if (req.iterator == null) {
            throw new AbstractServiceInvalidArgumentError('iterator');
        }
        let iterator = this.iterators.get(req.iterator);
        if (!iterator) {
            throw new AbstractServiceNotFoundError('iterator.' + req.iterator);
        }
        if (iterator.done) {
            throw new AbstractServiceResourceExhaustedError('iterator.' + req.iterator);
        }
        req.iterator = iterator;
    }

    deserializeValues(req, fromUpdate) {
        req.values = utils.remap(
            req.values ?? {},
            (k, v) => {
                v = utils.fromJson(v, undefined);
                v = (!fromUpdate && v == null) ? undefined : v;
                if (v === undefined) {
                    return;
                }
                return [k, v];
            },
        );
    }

    changeError(err, req) {
        if (err instanceof RepositoryError) {
            let msg = [err.constructor.name, req.repository, err.message];
            msg = msg.filter((e) => e != null).join(': ');
            let c = AbstractServiceInternalError;
            if (
                err instanceof RepositoryInvalidFieldError ||
                err instanceof RepositoryIdNotExistsError
            ) {
                c = AbstractServiceNotFoundError;
            }
            return new c(msg);
        }
    }
}

exports.StorageService = StorageService;

class StorageServiceServer extends AbstractServiceServer {
}

exports.StorageServiceServer = StorageServiceServer;

class StorageServiceClient extends AbstractServiceClient {
    InsertPreProcess(req) {
        req.values = utils.remap(req.values ?? {}, (k, v) => v != null ? [k, utils.toJson(v)] : null);
        return req;
    }

    UpdatePreProcess(req) {
        req.values = utils.remap(req.values ?? {}, (k, v) => v !== undefined ? [k, utils.toJson(v)] : null);
        return req;
    }

    GetPostProcess(res) {
        res.records = (res.records ?? []).map((rec) => {
            rec.values = utils.remap(rec.values ?? {}, (k, v) => {
                v = utils.fromJson(v, null);
                return v != null ? [k, v] : null;
            });
            return rec;
        });
        return res;
    }

    NextPostProcess(res) {
        return this.GetPostProcess(res);
    }

    IteratePreProcess(req) {
        if (utils.isObject(req.query)) {
            let map = ([k, v]) => `(@${k}:"${v.replace(/"/g, '\\"')}")`;
            req.query = Object.entries(req.query).map(map).join(' & ');
        }
        return req;
    }

    IteratePostProcess(res) {
        let client = this;
        return {
            ...res,
            async *iterate(limit, chunk) {
                limit = limit ?? this.default.limit;
                chunk = chunk ?? this.default.chunk;
                while (true) {
                    let {records, done, error} = await client.Next({
                        iterator: this.iterator,
                        limit,
                    }).catch((error) => ({error}));
                    if (error || !records.length) {
                        break;
                    }
                    yield* (chunk ? [records] : records);
                    if (done) {
                        break;
                    }
                }
            },
            default: {
                limit: 10,
                chunk: false,
            },
            [Symbol.asyncIterator]() {
                return this.iterate();
            },
        };
    }
}

exports.StorageServiceClient = StorageServiceClient;

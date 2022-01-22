const utils = require('ejz-utils');

const {StorageRpcServer} = require('./StorageRpcServer');

class StorageRpcClient {
    constructor(options) {
        this.client = utils.getRpcClient(options);
        let f = (method) => {
            let [char] = method;
            return (
                (char.toUpperCase() == char) &&
                (!method.endsWith('PreProcess')) &&
                (!method.endsWith('PostProcess'))
            );
        };
        let calls = Object.getOwnPropertyNames(StorageRpcServer.prototype).filter(f);
        for (let call of calls) {
            let [pre, post] = [`${call}PreProcess`, `${call}PostProcess`];
            this[call] = async (...args) => {
                if (this[pre]) {
                    args = await this[pre](args);
                }
                let resp = await this.client.call(call, ...args);
                if (!resp) {
                    throw new StorageRpcClientEmptyResponseError();
                }
                if (resp.error != null) {
                    throw new StorageRpcClientError(resp.error);
                }
                if (this[post]) {
                    resp = await this[post](resp, args);
                }
                return resp;
            };
        }
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

    IteratePreProcess(args) {
        let [req] = args;
        if (utils.isObject(req?.query)) {
            let map = ([k, v]) => {
                if (Array.isArray(v)) {
                    return `(@${k}:["${String(v[0]).replace(/"/g, '\\"')}","${String(v[1]).replace(/"/g, '\\"')}"])`;
                } else {
                    return `(@${k}:"${String(v).replace(/"/g, '\\"')}")`;
                }
            };
            req.query = Object.entries(req.query).map(map).join(' & ');
        }
        return args;
    }

    isAlive() {
        return this.Ping().then(({pinged}) => !!pinged).catch(() => false);
    }

    end() {
        return this.client.end();
    }
}

exports.StorageRpcClient = StorageRpcClient;

class StorageRpcClientError extends Error {}

exports.StorageRpcClientError = StorageRpcClientError;

class StorageRpcClientEmptyResponseError extends StorageRpcClientError {}

exports.StorageRpcClientEmptyResponseError = StorageRpcClientEmptyResponseError;

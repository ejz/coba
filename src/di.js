const utils = require('ejz-utils');

const {Storage} = require('./Storage');
const {Repository} = require('./Repository');

const _StorageRpcServer = require('./StorageRpcServer');
const _StorageRpcClient = require('./StorageRpcClient');

const {StorageRpcServer} = _StorageRpcServer;
const {StorageRpcClient} = _StorageRpcClient;

let onShutdownHooks = [];

async function onShutdown() {
    let onShutdownHooksCopy = onShutdownHooks;
    onShutdownHooks = [];
    for (let hook of onShutdownHooksCopy) {
        await hook();
    }
}

onShutdown.push = (cb) => onShutdownHooks.push(cb);

exports.onShutdown = onShutdown;

Object.assign(exports, _StorageRpcServer);
Object.assign(exports, _StorageRpcClient);

function makeLogger(options) {
    options = options ?? {};
    options.name = options.name ?? null;
    return utils.makeLogger(options);
}

exports.makeLogger = makeLogger;

function makeStorage(options) {
    options = options ?? {};
    let name = 'Storage';
    options.logger = options.logger instanceof utils.Logger ? options.logger : makeLogger({name, ...options.logger});
    options.root = options.root ?? utils.tempDirectory();
    let storage = new Storage({
        logger: options.logger,
        root: options.root,
    });
    onShutdown.push(() => storage.sync());
    return storage;
}

exports.makeStorage = makeStorage;

function makeRepository(options) {
    options = options ?? {};
    options.name = options.name ?? 'repository' + utils.ms();
    options.fields = options.fields ?? {};
    options.root = options.root ?? utils.tempDirectory();
    let repository = new Repository(
        options.name,
        options.fields,
        {
            root: options.root,
        },
    );
    onShutdown.push(() => repository.sync());
    return repository;
}

exports.makeRepository = makeRepository;

function makeStorageRpcServer(options) {
    options = options ?? {};
    options.interf = options.interf ?? null;
    let name = 'StorageRpc';
    options.logger = options.logger instanceof utils.Logger ? options.logger : makeLogger({name, ...options.logger});
    options.storage = options.storage instanceof Storage ? options.storage : makeStorage(options.storage);
    options.v8Serializer = options.v8Serializer ?? true;
    options.objectShallowCopy = options.objectShallowCopy ?? utils.isJestMode();
    let _constructor = options._constructor ?? StorageRpcServer;
    delete options._constructor;
    let server = new _constructor(options);
    server.promise.then(() => {
        onShutdown.push(() => server.sync());
        onShutdown.push(() => server.close());
    });
    return server.promise;
}

exports.makeStorageRpcServer = makeStorageRpcServer;

function makeStorageRpcClient(options) {
    options = options ?? {};
    options.interf = options.interf ?? null;
    options.v8Serializer = options.v8Serializer ?? true;
    options.objectShallowCopy = options.objectShallowCopy ?? utils.isJestMode();
    let _constructor = options._constructor ?? StorageRpcClient;
    delete options._constructor;
    let client = new _constructor(options);
    onShutdown.push(() => client.end());
    return client;
}

exports.makeStorageRpcClient = makeStorageRpcClient;

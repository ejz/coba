const path = require('path');

const utils = require('ejz-utils');
const grpc = require('ejz-grpc');

const {Storage} = require('./Storage');
const {Repository} = require('./Repository');

const _StorageService = require('./StorageService');

const {
    StorageService,
    StorageServiceServer,
    StorageServiceClient,
} = _StorageService;

let onShutdownHooks = [grpc.onShutdown];

async function onShutdown() {
    let onShutdownHooksCopy = onShutdownHooks;
    onShutdownHooks = [grpc.onShutdown];
    for (let hook of onShutdownHooksCopy) {
        await hook();
    }
}

onShutdown.push = (cb) => onShutdownHooks.push(cb);

exports.onShutdown = onShutdown;

Object.assign(exports, _StorageService);

function makeLogger(options) {
    options = options ?? {};
    options.name = options.name ?? null;
    return utils.makeLogger(options);
}

exports.makeLogger = makeLogger;

function getStorageServiceProto() {
    let proto = path.resolve(__dirname, 'StorageService.proto');
    return grpc.getProto(proto);
}

exports.getStorageServiceProto = getStorageServiceProto;

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

function makeStorageService(options) {
    options = options ?? {};
    let name = 'StorageService';
    options.logger = options.logger instanceof utils.Logger ? options.logger : makeLogger({name, ...options.logger});
    options.storage = options.storage instanceof Storage ? options.storage : makeStorage(options.storage);
    let service = new StorageService(options.logger, options.storage);
    onShutdown.push(() => service.sync());
    return service;
}

exports.makeStorageService = makeStorageService;

async function makeStorageServiceServer(options) {
    options = options ?? {};
    options.interf = options.interf ?? null;
    options.storageService = options.storageService instanceof StorageService ? options.storageService : makeStorageService(options.storageService);
    let server = new StorageServiceServer(options.interf);
    server.addService(grpc.getAbstractServiceProto(), options.storageService);
    let proto = getStorageServiceProto();
    server.addService(proto, options.storageService);
    await server.start();
    onShutdown.push(() => server.stop());
    return server;
}

exports.makeStorageServiceServer = makeStorageServiceServer;

function makeStorageServiceClient(options) {
    options = options ?? {};
    options.interf = options.interf ?? null;
    options._constructor = options._constructor ?? StorageServiceClient;
    let client = new options._constructor(options.interf);
    client.addService(grpc.getAbstractServiceProto());
    let proto = getStorageServiceProto();
    client.addService(proto);
    return client;
}

exports.makeStorageServiceClient = makeStorageServiceClient;

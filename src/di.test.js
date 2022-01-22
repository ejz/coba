const utils = require('ejz-utils');

const {Storage} = require('./Storage');
const {Repository} = require('./Repository');

const {StorageRpcServer} = require('./StorageRpcServer');
const {StorageRpcClient} = require('./StorageRpcClient');

const {
    makeStorage,
    makeRepository,
    makeStorageRpcServer,
    makeStorageRpcClient,
    onShutdown,
} = require('./di');

afterEach(onShutdown);

test('di / makeStorage', () => {
    expect(makeStorage() instanceof Storage).toEqual(true);
});

test('di / makeRepository', () => {
    expect(makeRepository() instanceof Repository).toEqual(true);
});

test('di / makeStorageRpcServer', async () => {
    let interf = utils.getRandomInterface();
    let server = await makeStorageRpcServer({interf});
    expect(server instanceof StorageRpcServer).toEqual(true);
});

test('di / makeStorageRpcClient', async () => {
    let interf = utils.getRandomInterface();
    let client = makeStorageRpcClient({interf});
    expect(client instanceof StorageRpcClient).toEqual(true);
});

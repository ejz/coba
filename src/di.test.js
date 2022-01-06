const utils = require('ejz-utils');

const {Storage} = require('./Storage');
const {Repository} = require('./Repository');

const {
    StorageService,
    StorageServiceServer,
    StorageServiceClient,
} = require('./StorageService');

const {
    makeStorage,
    makeRepository,
    makeStorageService,
    makeStorageServiceServer,
    makeStorageServiceClient,
    onShutdown,
} = require('./di');

afterEach(onShutdown);

test('di / makeStorage', () => {
    expect(makeStorage() instanceof Storage).toEqual(true);
});

test('di / makeRepository', () => {
    expect(makeRepository() instanceof Repository).toEqual(true);
});

test('di / makeStorageService', () => {
    expect(makeStorageService() instanceof StorageService).toEqual(true);
});

test('di / makeStorageServiceServer', async () => {
    let interf = utils.getRandomInterface();
    let server = await makeStorageServiceServer({interf});
    expect(server instanceof StorageServiceServer).toEqual(true);
});

test('di / makeStorageServiceClient', async () => {
    let interf = utils.getRandomInterface();
    let client = makeStorageServiceClient({interf});
    expect(client instanceof StorageServiceClient).toEqual(true);
});

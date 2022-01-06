const {
    makeStorage,
    onShutdown,
} = require('./di');

afterEach(onShutdown);

test('Storage / common / 1', () => {
    let s = makeStorage();
    expect(s.list()).toEqual([]);
    s.create('rep');
    expect(s.list()).toEqual(['rep']);
    expect(s.getRepository('rep') != null).toEqual(true);
});

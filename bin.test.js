const path = require('path');

const {exec: _exec, spawn} = require('child_process');
const utils = require('ejz-utils');

const VERSION = require('./package').version;

const ROOT = path.resolve(__dirname);
const BIN = path.resolve(ROOT, 'bin.js');

let hooks = [];

afterAll(async () => {
    for (let hook of hooks) {
        await hook();
    }
});

async function start(options) {
    let dir = utils.tempDirectory();
    options = options ?? {};
    options.config = options.config ?? {};
    options.config.root = options.config.root ?? '.';
    options.config.interface = options.config.interface ?? utils.getRandomInterface();
    let content = Object.entries(options.config).map(([k, v]) => {
        return `${k} = ${utils.toJson(v)}`;
    }).join('\n') + '\n';
    let config = path.resolve(dir, utils.rand() + '');
    utils.writeFile(config, content);
    let api = {
        _stdout: [],
        _stderr: [],
        get stdout() {
            return this._stdout.map(String).join('').trim();
        },
        get stderr() {
            return this._stderr.map(String).join('').trim();
        },
    };
    api.reset = () => (api._stdout = [], api._stderr = []);
    if (options.spawn ?? true) {
        let child = spawn(BIN, ['-C', config, 'start']);
        child.stdout.on('data', (c) => api._stdout.push(c));
        child.stderr.on('data', (c) => api._stderr.push(c));
        api.kill = () => child.kill();
        hooks.push(() => api.kill());
        await utils.sleep(200);
        expect(api.stdout).not.toMatch('ERR');
        expect(api.stdout).toMatch('STARTING');
        expect(api.stdout).toMatch('STARTED');
    }
    api.exec = (...args) => {
        let q = '\'';
        let sp = ' ';
        args = ['-C', config, ...args];
        return new Promise((r) => {
            _exec(BIN + sp + q + args.join(q + sp + q) + q + sp, (error, stdout, stderr) => {
                r({stdout, stderr, error});
            });
        });
    };
    api.ok = async (...args) => {
        let r = await api.exec(...args);
        expect(r.error).toBeFalsy();
        return String(r.stdout).trim();
    };
    api.err = async (...args) => {
        let r = await api.exec(...args);
        expect(r.error).toBeTruthy();
        return String(r.stdout).trim();
    };
    api.options = options;
    return api;
}

test('coba / bin.js / common / 1', async () => {
    expect(utils.isFile(BIN)).toEqual(true);
    let api = await start();
    expect(await api.ok('help')).toMatch('->');
    expect(await api.ok('version')).toMatch(/version/i);
    expect(await api.ok('version')).toMatch(VERSION);
    await api.err();
    await api.err('');
    await api.err(utils.rand());
    await api.err('st');
    await api.err('sta');
});

test('coba / bin.js / common / 2', async () => {
    let api = await start({spawn: false});
    expect(await api.err('status')).toMatch(/no\s+connection/i);
    api = await start();
    expect(await api.ok('status')).not.toMatch(/no\s+connection/i);
    expect(await api.ok('status')).toMatch('OK');
    api.kill();
    await utils.sleep(100);
    expect(api.stdout).toMatch(/signal/i);
    expect(api.stdout).toMatch(/exit/i);
});

test('coba / bin.js / config', async () => {
    let api = await start();
    let config = utils.fromJson(await api.ok('config'));
    expect(utils.isDirectory(config.root)).toEqual(true);
    let root = await api.ok('config', 'root');
    expect(utils.isDirectory(root)).toEqual(true);
});

test('coba / bin.js / logger-level', async () => {
    let api = await start();
    expect(utils.isJestMode()).toEqual(true);
    expect(await api.ok('get-logger-level')).toMatch('NONE');
    expect(await api.ok('set-logger-level', 'DBG')).toMatch('TRUE');
    expect(await api.ok('get-logger-level')).toMatch('DBG');
    await api.ok('set-logger-level', 'NONE');
    api.reset();
    await api.ok('status');
    expect(api.stdout).toMatch(/^$/);
    await api.ok('set-logger-level', 'DBG');
    api.reset();
    await api.ok('status');
    expect(api.stdout).not.toMatch(/^$/);
});

test('coba / bin.js / create, list, drop', async () => {
    let api = await start();
    await api.ok('set-logger-level', 'DBG');
    expect(utils.nsplit(await api.ok('list'))).toEqual([]);
    expect(await api.ok('create', 'rep1')).toMatch('TRUE');
    expect(await api.ok('create', 'rep2')).toMatch('TRUE');
    expect(utils.nsplit(await api.ok('list'))).toEqual(['rep1', 'rep2']);
    expect(await api.ok('drop', 'rep1')).toMatch('TRUE');
    expect(utils.nsplit(await api.ok('list'))).toEqual(['rep2']);
});

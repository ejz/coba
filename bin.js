#!/usr/bin/env node

const os = require('os');
const fs = require('fs');
const path = require('path');

const tcpPortUsed = require('tcp-port-used');
const utils = require('ejz-utils');

const VERSION = require('./package').version;
const [BIN] = Object.keys(require('./package').bin);

const ROOT = __dirname;
const CWD = process.env._CWD ?? process.cwd();
const IS_ROOT = process.env._IS_ROOT ?? (os.userInfo().uid === 0);
const HOME = process.env._HOME ?? os.homedir();
const VARLIB = process.env._VARLIB ?? '/var/lib';

const DI = path.resolve(ROOT, 'di.js');
const DI_ALT = path.resolve(ROOT, 'src', 'di.js');
const di = require(utils.isFile(DI) ? DI : DI_ALT);

const CONFIG_FILE = 'config.ini';
const CONFIG_DIR = 'coba';
const CONFIG_DIR_DOT = '.coba';

const CONFIG_DEFAULT = path.resolve(ROOT, CONFIG_FILE);
const CONFIG_DEFAULT_ALT = path.resolve(ROOT, 'etc', CONFIG_FILE);
const CONFIG_HOME = (!IS_ROOT && utils.isDirectory(HOME)) ? path.resolve(HOME, CONFIG_DIR_DOT, CONFIG_FILE) : null;
const CONFIG_VARLIB = (IS_ROOT && utils.isDirectory(VARLIB)) ? path.resolve(VARLIB, CONFIG_DIR, CONFIG_FILE) : null;

let _v_c_d = utils.isFile(CONFIG_DEFAULT);
let _v_c_d_a = utils.isFile(CONFIG_DEFAULT_ALT);

if (!((+_v_c_d) ^ (+_v_c_d_a))) {
    console.log('INVALID DEFAULT CONFIG');
    process.exit(1);
}

const _CONFIG_DEFAULT = _v_c_d ? CONFIG_DEFAULT : CONFIG_DEFAULT_ALT;

const CONFIGS = [_CONFIG_DEFAULT];

const OK = '[OK]';
const ERR = '[ERR]';

const uncaught = (prefix) => (e) => console.log(prefix, String(e));

process.on('uncaughtException', uncaught('uncaughtException:'));
process.on('unhandledRejection', uncaught('unhandledRejection:'));

const onSignal = async (signal) => {
    console.log('SIGNAL:', signal);
    await di.onShutdown();
    console.log('EXIT');
    process.exit(1);
};

process.on('SIGINT', onSignal);
process.on('SIGTERM', onSignal);

let _config = CONFIG_HOME ?? CONFIG_VARLIB;
_config = utils.isFile(_config) ? _config : null;

let argv = process.argv.slice(2);
let opts = {
    config: {
        type: 'string',
        alias: 'C',
        default: _config,
    },
    ...utils.combine(Object.keys(getConfig()), {type: 'string'}),
};

try {
    argv = utils.argv(argv, opts);
} catch (e) {
    console.log(utils.stringifyError(e, true));
    process.exit(1);
}

if (argv.config != null) {
    argv.config = path.resolve(CWD, argv.config);
    if (utils.isFile(argv.config)) {
        CONFIGS.push(argv.config);
    }
}

let actions = Object.create(null);

function actionVersion() {
    console.log('version:', VERSION);
}

actions.version = {action: actionVersion, help: 'version'};

function actionHelp(filter) {
    for (let [k, {help}] of Object.entries(actions)) {
        if (help && (filter == null || filter == k)) {
            console.log('->', help);
        }
    }
}

actions.help = {action: actionHelp, help: 'help'};

async function actionConfig() {
    let config = getConfig();
    let key = argv._.shift();
    if (key == null) {
        console.log(utils.toJson(config, true));
    } else if (config[key]) {
        console.log(config[key]);
    }
}

actions.config = {action: actionConfig, help: 'config [key]', nargs: [0, 1]};

async function actionStatus() {
    console.log(OK);
}

actions.status = {action: actionStatus, help: 'status', client: true};

async function actionSetLoggerLevel(client) {
    let level = argv._.shift();
    level = String(level).toUpperCase();
    let res = await client.SetLoggerLevel({level}).catch(String);
    if (typeof(res) == 'string') {
        console.log(ERR, res);
        return 1;
    }
    console.log('SET', String(res.set).toUpperCase());
}

actions['set-logger-level'] = {action: actionSetLoggerLevel, client: true, help: 'set-logger-level [level]', alias: ['set-log-level'], nargs: [1, 1]};

async function actionGetLoggerLevel(client) {
    let res = await client.GetLoggerLevel().catch(String);
    if (typeof(res) == 'string') {
        console.log(ERR, res);
        return 1;
    }
    console.log('LEVEL', res.level);
}

actions['get-logger-level'] = {action: actionGetLoggerLevel, help: 'get-logger-level', alias: ['get-log-level'], client: true};

async function actionLock(client) {
    let res = await client.Lock().catch(String);
    if (typeof(res) == 'string') {
        console.log(ERR, res);
        return 1;
    }
    console.log('LOCKED', String(res.locked).toUpperCase());
}

actions.lock = {action: actionLock, help: 'lock', client: true};

async function actionUnlock(client) {
    let res = await client.Unlock().catch(String);
    if (typeof(res) == 'string') {
        console.log(ERR, res);
        return 1;
    }
    console.log('UNLOCKED', String(res.unlocked).toUpperCase());
}

actions.unlock = {action: actionUnlock, help: 'unlock', client: true};

async function actionStart() {
    console.log('STARTING', '..');
    let {interface: interf, root} = getConfig();
    if (!await isInterfaceFree(interf)) {
        console.log(ERR, 'interface is occupied:', interf);
        return 1;
    }
    await di.makeStorageRpcServer({
        interf,
        storage: {root},
    });
    console.log(OK, 'STARTED', '..', interf);
}

actions.start = {action: actionStart, help: 'start', listen: true};

async function actionCreate(client) {
    let repository = argv._.shift();
    let res = await client.Create({repository}).catch(String);
    if (typeof(res) == 'string') {
        console.log(ERR, res);
        return 1;
    }
    console.log('CREATED', String(res.created).toUpperCase());
}

actions.create = {action: actionCreate, help: 'create [repository]', nargs: [1, 1], client: true};

async function actionList(client) {
    let res = await client.List().catch(String);
    if (typeof(res) == 'string') {
        console.log(ERR, res);
        return 1;
    }
    for (let repository of res.repositories) {
        console.log(repository);
    }
}

actions.list = {action: actionList, help: 'list', client: true};

async function actionDrop(client) {
    let repository = argv._.shift();
    let res = await client.Drop({repository}).catch(String);
    if (typeof(res) == 'string') {
        console.log(ERR, res);
        return 1;
    }
    console.log('DROPPED', String(res.dropped).toUpperCase());
}

actions.drop = {action: actionDrop, help: 'drop [repository]', nargs: [1, 1], client: true};

async function actionInitConfig() {
    let config = argv.config;
    if (!config) {
        console.log(ERR, 'INVALID USER CONFIG');
        return 1;
    }
    let directory = path.dirname(config);
    utils.mkdir(directory);
    if (utils.isFile(config)) {
        console.log(ERR, config, 'EXISTS');
        return 1;
    }
    utils.writeFile(config, utils.readFile(_CONFIG_DEFAULT));
    console.log(utils.isFile(config) ? OK : ERR, config);
}

actions['init-config'] = {action: actionInitConfig, help: 'init-config'};

//
// --- --- ---
//

let action = argv._.shift();

if (action == null) {
    console.log(`Try '${BIN} help' for more information.`);
    process.exit(1);
}

let before = utils.remap(actions, (k, v) => {
    if (typeof(v) == 'function' && k.startsWith('before-')) {
        return [k.substring(7), v];
    }
});

let after = utils.remap(actions, (k, v) => {
    if (typeof(v) == 'function' && k.startsWith('after-')) {
        return [k.substring(6), v];
    }
});

actions = utils.filter(actions, (k, v) => typeof(v) != 'function');

let possible = guess(actions, action);

if (!possible) {
    console.log('Unknown Action:', "'" + action + "'");
    process.exit(1);
}

if (possible.length != 1) {
    console.log('Ambiguous Actions:', "'" + possible.join("', '") + "'");
    process.exit(1);
}

possible = possible.shift();

action = actions[possible];
action.nargs = action.nargs ?? [0, 0];
action.requireConfig = action.requireConfig ?? false;
let [amin, amax] = [action.nargs[0] ?? 0, action.nargs[1] ?? 1E9];

if (!(amin <= argv._.length && argv._.length <= amax)) {
    actionHelp(possible);
    process.exit(1);
}

if (action.requireConfig && CONFIGS.length < 2) {
    console.log('REQUIRED CONFIG. RUN \'init-config\'');
    process.exit(1);
}

before = before[possible] ?? null;
after = after[possible] ?? null;
(async () => {
    let c = 0;
    if (!action.client) {
        if (before != null && c == 0) {
            c = (await before()) ?? 0;
        }
        if (c == 0) {
            c = (await action.action()) ?? 0;
        }
        if (after != null && c == 0) {
            c = (await after()) ?? 0;
        }
    } else {
        let client = getClient();
        if (!client || !(await client.isAlive())) {
            console.log(ERR, 'NO CONNECTION');
            c = 1;
        }
        if (before != null && c == 0) {
            c = (await before(client)) ?? 0;
        }
        if (c == 0) {
            c = (await action.action(client)) ?? 0;
        }
        if (after != null && c == 0) {
            c = (await after(client)) ?? 0;
        }
    }
    if (!action.listen || c != 0) {
        process.exit(c);
    }
})();

//
// --- --- ---
//

function getClient() {
    let interf = getConfig().interface;
    return di.makeStorageRpcClient({interf});
}

function getConfig() {
    let final = {};
    for (let config of CONFIGS) {
        final = {...final, ...parseConfig(config)};
    }
    for (let [k, v] of Object.entries(argv ?? {})) {
        if (k == '_' || final[k] == null) {
            continue;
        }
        final[k] = utils.fromJson(v, v);
        if (['root'].includes(k)) {
            final[k] = path.resolve(CWD, final[k]);
        }
    }
    return final;
}

function parseConfig(file) {
    let content = String(utils.readFile(file));
    if (!content) {
        return {};
    }
    let lines = utils.nsplit(content).filter((line) => !line.startsWith(';'));
    let config = {};
    for (let line of lines) {
        let [k, v] = line.split(/\s*=\s*/);
        config[k] = utils.fromJson(v, v);
        if (['root'].includes(k)) {
            config[k] = path.resolve(path.dirname(file), config[k]);
        }
    }
    return config;
}

function guess(dict, search) {
    if (!dict || !search) {
        return null;
    }
    search = search.toLowerCase();
    let entries = Object.entries(dict).map(([k, v]) => [k.toLowerCase(), v]);
    let cases = [
        ([k]) => k == search,
        ([k, v]) => (v.alias || []).includes(search),
        ([k]) => k.startsWith(search),
    ];
    for (let cas of cases) {
        let found = entries.filter(cas);
        if (!found.length) {
            continue;
        }
        return found.map(([k]) => k);
    }
    return null;
}

function isInterfaceFree(interf) {
    let [i, p] = interf.split(':');
    return tcpPortUsed.check(+p, i).then((r) => !r).catch(() => true);
}

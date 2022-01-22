const fs = require('fs');
const path = require('path');

const utils = require('ejz-utils');

const {Repository} = require('./Repository');

const {
    DELETED_PREFIX,
    CORRUPTED_PREFIX,
    META_FILE,
    REPOSITORY_NAME_REGEX,
    FIELD_ID,
    FIELD_NAME_REGEX,
} = require('./constants');

class Storage {
    constructor(options) {
        options = options ?? {};
        options.root = options.root ?? null;
        utils.ok.dir(options.root);
        utils.ok.instance(utils.Logger, options.logger);
        let root = (...parts) => path.resolve(options.root, ...parts);
        root.meta = (meta) => this.root(meta, META_FILE);
        this.root = root;
        this.logger = options.logger;
        this.repositories = Object.create(null);
        this.initRepositories();
    }

    initRepositories() {
        let list = utils.listDirectory(this.root()).map((f) => path.basename(f));
        list = list.filter((r) => !r.startsWith(DELETED_PREFIX) && !r.startsWith(CORRUPTED_PREFIX));
        for (let name of list) {
            if (!utils.isDirectory(this.root(name))) {
                continue;
            }
            let file = this.root.meta(name);
            if (!utils.isFile(file)) {
                continue;
            }
            let meta = utils.fromJson(String(utils.readFile(file)), null);
            if (!meta) {
                this.logger.err(`Could not read META for "${name}"`);
                continue;
            }
            this.logger.log(`Creating repository "${name}" ..`);
            try {
                this.repositories[name] = this.newRepository(name, meta);
                this.logger.log(`Created repository "${name}"`);
            } catch (e) {
                this.logger.log(`Errored creating repository "${name}"`);
            }
        }
        this.initRepositoriesRepositories();
    }

    newRepository(name, meta) {
        let repository = new Repository(name, meta.fields ?? {}, {
            root: this.root(name),
        });
        return repository;
    }

    initRepositoriesRepositories() {
        for (let repository of Object.values(this.repositories)) {
            repository.repositories = this.repositories;
        }
    }

    list() {
        return Object.keys(this.repositories);
    }

    exists(repository) {
        return this.repositories[repository] != null;
    }

    create(name, fields) {
        fields = fields ?? {};
        if (this.repositories[name]) {
            this.logger.err(`create(): repository already exists: "${name}"`);
            return false;
        }
        if (!REPOSITORY_NAME_REGEX.test(name)) {
            this.logger.err(`create(): invalid repository name: "${name}"`);
            return false;
        }
        for (let [field, {type}] of Object.entries(fields)) {
            if (field == FIELD_ID) {
                this.logger.err(`create(): reserved field identificator: "${field}"`);
                return false;
            }
            if (!FIELD_NAME_REGEX.test(field)) {
                this.logger.err(`create(): invalid field identificator: "${field}"`);
                return false;
            }
            if (!type) {
                this.logger.err(`create(): invalid field type: "${field}"`);
                return false;
            }
        }
        let directory = this.root(name);
        if (utils.isDirectory(directory)) {
            let target = [CORRUPTED_PREFIX, name, process.hrtime().join('')].join('_');
            fs.renameSync(directory, this.root(target));
        }
        utils.mkdir(directory);
        let meta = {fields};
        utils.writeFile(this.root.meta(name), utils.toJson(meta, true) + '\n');
        this.repositories[name] = this.newRepository(name, meta);
        this.initRepositoriesRepositories();
        return true;
    }

    drop(name) {
        let repository = this.repositories[name];
        if (!repository) {
            this.logger.err(`drop(): repository not exists: "${name}"`);
            return false;
        }
        repository.sync();
        delete this.repositories[name];
        let target = [DELETED_PREFIX, name, process.hrtime().join('')].join('_');
        fs.renameSync(this.root(name), this.root(target));
        return true;
    }

    sync() {
        for (let repository of Object.values(this.repositories)) {
            repository.sync();
        }
    }

    getRepository(name) {
        return this.repositories[name] ?? null;
    }
}

exports.Storage = Storage;

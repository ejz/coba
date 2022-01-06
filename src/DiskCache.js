const fs = require('fs');
const path = require('path');

const utils = require('ejz-utils');

class DiskCache {
    constructor(options, logger) {
        options = options ?? {};
        options.ms = options.ms ?? 10E3;
        let key2file = (key) => path.resolve(String(key));
        options.key2file = options.key2file ?? key2file;
        this.options = options;
        this.logger = logger;
        this.cache = Object.create(null);
    }

    exists(key) {
        return this.cache[key] != null;
    }

    save(key, content) {
        let cache = this.cache[key] ?? this.newCacheEntry(key);
        cache.content = content ?? null;
    }

    load(key) {
        let cache = this.cache[key] ?? this.newCacheEntry(key, this.readContent(key));
        return cache.content;
    }

    newCacheEntry(key, content) {
        let debounced = utils.debounce(function() {
            this.sync();
        }, this.options.ms);
        let entry = {
            self: this,
            key,
            changed: false,
            sync() {
                if (this.changed) {
                    this.self.writeContent(this.key, this._content);
                }
                this.debounced.cancel();
                delete this.self.cache[this.key];
            },
            _content: (content ?? null),
            get content() {
                this.debounced();
                return this._content;
            },
            set content(content) {
                this.changed = true;
                this._content = content;
                this.debounced();
            },
            debounced,
        };
        this.cache[key] = entry;
        return entry;
    }

    sync(key) {
        if (key != null) {
            let cache = this.cache[key];
            if (cache != null) {
                cache.sync();
            }
        } else {
            for (let key in this.cache) {
                this.cache[key].sync();
            }
        }
    }

    readContent(key) {
        let file = this.options.key2file(key);
        if (file == null) {
            throw new DiskCacheFileError(key);
        }
        if (this.logger != null) {
            this.logger.log('readContent:', key);
        }
        let buffer;
        try {
            buffer = fs.readFileSync(file);
        } catch (e) {
            buffer = null;
        }
        if (this.buffer2content) {
            buffer = this.buffer2content(buffer);
        }
        return buffer;
    }

    writeContent(key, content) {
        let file = this.options.key2file(key);
        if (file == null) {
            throw new DiskCacheFileError(key);
        }
        if (this.logger != null) {
            this.logger.log('writeContent:', key);
        }
        if (this.content2buffer) {
            content = this.content2buffer(content);
        }
        try {
            if (content == null) {
                fs.unlinkSync(file);
            } else {
                fs.writeFileSync(file, content);
            }
        } catch (e) {
            return;
        }
    }
}

exports.DiskCache = DiskCache;

class DiskCacheError extends Error {}

exports.DiskCacheError = DiskCacheError;

class DiskCacheFileError extends DiskCacheError {}

exports.DiskCacheFileError = DiskCacheFileError;

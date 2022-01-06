const utils = require('ejz-utils');

const {Field} = require('./Field');
const {DiskCache} = require('./DiskCache');

class RecordDiskCache extends DiskCache {
    constructor(options, logger) {
        options = options ?? {};
        options.ms = options.ms ?? 30E3;
        super(options, logger);
        utils.ok.instance(Field, ...Object.values(this.options.fields));
    }

    content2buffer(content) {
        content = utils.filter(content, (k, v) => v != null);
        if (!Object.keys(content).length) {
            return null;
        }
        return utils.toJson(utils.map(content, (v, k) => {
            return this.options.fields[k].serializeToDisk(v);
        }));
    }

    buffer2content(buffer) {
        if (buffer == null) {
            return {};
        }
        return utils.map(utils.fromJson(buffer, {}), (v, k) => {
            return this.options.fields[k].deserializeFromDisk(v);
        });
    }
}

exports.RecordDiskCache = RecordDiskCache;

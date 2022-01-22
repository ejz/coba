const NOOP = (() => {});

exports.NOOP = NOOP;

const V2V = ((v) => v);

exports.V2V = V2V;

const LOGGER_NAME = 'coba';

exports.LOGGER_NAME = LOGGER_NAME;

const ITERATOR_TIMEOUT = 10 * 60 * 1E3;

exports.ITERATOR_TIMEOUT = ITERATOR_TIMEOUT;

const MAX = Math.pow(2, 32) - 1;

exports.MAX = MAX;

const DELAY = 3600E3;

exports.DELAY = DELAY;

const DELAY_MIN = 1E3;

exports.DELAY_MIN = DELAY_MIN;

const SHARDS = '0123456789abcedf'.split('').map((s) => s + '-');

exports.SHARDS = SHARDS;

const IDS_DIR = 'IDS';

exports.IDS_DIR = IDS_DIR;

const FIELDS_DIR = 'FIELDS';

exports.FIELDS_DIR = FIELDS_DIR;

const IDS_FILE = 'ids';

exports.IDS_FILE = IDS_FILE;

const AUTOID_FILE = 'autoid';

exports.AUTOID_FILE = AUTOID_FILE;

const BSI_ALL_FILE = 'all';

exports.BSI_ALL_FILE = BSI_ALL_FILE;

const META_FILE = 'META';

exports.META_FILE = META_FILE;

const DELETED_PREFIX = '0_deleted';

exports.DELETED_PREFIX = DELETED_PREFIX;

const CORRUPTED_PREFIX = '0_corrupted';

exports.CORRUPTED_PREFIX = CORRUPTED_PREFIX;

const REPOSITORY_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

exports.REPOSITORY_NAME_REGEX = REPOSITORY_NAME_REGEX;

const FIELD_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

exports.FIELD_NAME_REGEX = FIELD_NAME_REGEX;

const MS_IN_A_DAY = 3600 * 24 * 1E3;

exports.MS_IN_A_DAY = MS_IN_A_DAY;

const SEC_IN_A_DAY = 3600 * 24;

exports.SEC_IN_A_DAY = SEC_IN_A_DAY;

const _TRUE = ['true', '1', 'yes', 'y'];

exports._TRUE = _TRUE;

const _FALSE = ['false', '0', 'no', 'n'];

exports._FALSE = _FALSE;

const MIN_INT = Number.MIN_SAFE_INTEGER;

exports.MIN_INT = MIN_INT;

const MAX_INT = Number.MAX_SAFE_INTEGER;

exports.MAX_INT = MAX_INT;

const MIN_DATE = '1970-01-01';

exports.MIN_DATE = MIN_DATE;

const MAX_DATE = '2050-01-01';

exports.MAX_DATE = MAX_DATE;

const MIN_DATETIME = '1970-01-01 00:00:00';

exports.MIN_DATETIME = MIN_DATETIME;

const MAX_DATETIME = '2050-01-01 23:59:59';

exports.MAX_DATETIME = MAX_DATETIME;

const BITMAP_ALL_FILE = '__ALL__';

exports.BITMAP_ALL_FILE = BITMAP_ALL_FILE;

const IS_ARR_ADD_REM = (k) => (
    k.endsWith(']') && (
        k.endsWith('[]') ||
        k.endsWith('[+]') ||
        k.endsWith('[+!]') ||
        k.endsWith('[!+]') ||
        k.endsWith('[-]') ||
        k.endsWith('[-!]') ||
        k.endsWith('[!-]')
    )
);

exports.IS_ARR_ADD_REM = IS_ARR_ADD_REM;

const IS_ARR_BEG = (k) => k.endsWith('!]');

exports.IS_ARR_BEG = IS_ARR_BEG;

const IS_ARR_REM = (k) => k.endsWith('-]') || k.endsWith('-!]');

exports.IS_ARR_REM = IS_ARR_REM;

const TRIM_ARR_ADD_REM = (k) => k.slice(0, k.lastIndexOf('['));

exports.TRIM_ARR_ADD_REM = TRIM_ARR_ADD_REM;

const SPLIT_ARR_ADD_REM = (o) => Object.entries(o).reduce((a, [k, v]) => {
    a[+k.startsWith('add')].push(...v);
    return a;
}, [[], []]);

exports.SPLIT_ARR_ADD_REM = SPLIT_ARR_ADD_REM;

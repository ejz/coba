const utils = require('ejz-utils');

const {Terms} = require('./Terms');
const {Tokenizer} = require('./Tokenizer');

const rules = {
    FK: [
        /^@@([a-zA-Z_][a-zA-Z0-9_]*)\s*:/,
        (m) => ['', m[1]],
    ],
    FK_ALONE: [
        /^@@([a-zA-Z_][a-zA-Z0-9_]*)/,
        (m) => ['', m[1]],
    ],
    IDENT: [
        /^@([a-zA-Z_][a-zA-Z0-9_]*)\s*:/,
        (m) => ['', m[1]],
    ],
    IDENT_COMPARE: [
        /^@([a-zA-Z_][a-zA-Z0-9_]*)\s*(>=|<=|>|<)/,
        (m) => ['', [m[1], m[2]]],
    ],
    IDENT_ALONE: [
        /^@([a-zA-Z_][a-zA-Z0-9_]*)/,
        (m) => ['', m[1]],
    ],
    NUMERIC: [
        /^([+-]?[0-9]+(\.[0-9]+)?)(?![a-zA-Z_0-9])/,
        (m) => ['', m[1]],
    ],
    VALUE: [
        /^([a-zA-Z_0-9]+)/,
        (m) => ['', m[1]],
    ],
    ALL: [
        /^(\*)/,
        () => [''],
    ],
    // & - AND
    // | - OR
    // () - para
    // , - comma
    // - - negation
    SPECIAL: [
        /^([&|(),-])/,
        (m) => ['', m[1]],
    ],
    ENTER_QUOTE_MODE: [
        /^"/,
        () => ['', null, 'QUOTE'],
    ],
    EXIT_QUOTE_MODE: [
        /^"/,
        () => ['', null, ''],
        'QUOTE',
    ],
    QUOTED_VALUE: [
        /^([^"\\]+|\\"|\\\\|\\)/,
        (m) => ['', m[1]],
        'QUOTE',
    ],
    ENTER_SQUOTE_MODE: [
        /^'/,
        () => ['', null, 'SQUOTE'],
    ],
    EXIT_SQUOTE_MODE: [
        /^'/,
        () => ['', null, ''],
        'SQUOTE',
    ],
    SQUOTED_VALUE: [
        /^([^'\\]+|\\'|\\\\|\\)/,
        (m) => ['', m[1]],
        'SQUOTE',
    ],
    RANGE_OPEN: [
        /^\[/,
        () => [''],
    ],
    RANGE_CLOSE: [
        /^\]/,
        () => [''],
    ],
    SPACE: [
        /^\s+/,
        () => [''],
    ],
};

class QueryParser {
    constructor() {
        this.tokenizer = new Tokenizer(rules);
    }

    tokenize(string) {
        return this.tokenizer.tokenize(string);
    }

    normalize(tokens, level) {
        level = level ?? 5;
        if (level >= 1) {
            tokens = this.normalizeTokenType(tokens);
        }
        if (level >= 2) {
            tokens = this.normalizeMergeQuote(tokens);
        }
        if (level >= 3) {
            tokens = this.normalizeMergeRange(tokens);
        }
        if (level >= 4) {
            tokens = this.normalizeMergePrefix(tokens);
        }
        if (level >= 5) {
            tokens = this.normalizeMergeIdentCompare(tokens);
        }
        return tokens;
    }

    normalizeTokenType(tokens) {
        for (let token of tokens) {
            if (token.type == 'NUMERIC') {
                token.type = 'VALUE';
            }
            if (token.type == 'SPECIAL') {
                token.type = token.value;
                token.special = true;
                delete token.value;
            }
        }
        return tokens;
    }

    normalizeMergeQuote(tokens) {
        let value;
        let filter = (token) => {
            switch (token.type) {
                case 'ENTER_QUOTE_MODE':
                case 'ENTER_SQUOTE_MODE':
                    value = [];
                    return false;
                case 'QUOTED_VALUE':
                case 'SQUOTED_VALUE':
                    value.push(token.value);
                    return false;
                case 'EXIT_QUOTE_MODE':
                case 'EXIT_SQUOTE_MODE':
                    if (token.type == 'EXIT_QUOTE_MODE') {
                        token.value = value.join('').replace(/\\("|\\)/g, '$1');
                        token.quoted = true;
                    } else {
                        token.value = value.join('').replace(/\\('|\\)/g, '$1');
                        token.squoted = true;
                    }
                    token.type = 'VALUE';
                    return true;
                default:
                    return true;
            }
        };
        tokens = tokens.filter(filter);
        return tokens;
    }

    normalizeMergeRange(tokens) {
        let range;
        let validator = (tokens) => {
            tokens = tokens.filter((t) => t.type != 'SPACE');
            let result = [false, null, null, false];
            if (tokens[0].type == '(') {
                tokens.shift();
                result[0] = true;
            }
            let l = tokens.length;
            if (tokens[l - 1].type == ')') {
                tokens.pop();
                result[3] = true;
                l--;
            }
            let type = tokens.map((t) => t.type).join('');
            if (/^VALUE$/.test(type)) {
                if (result[0] || result[3]) {
                    throw new QueryParserInvalidSyntaxError();
                }
                result[1] = tokens[0].value;
                result[2] = tokens[0].value;
                return result;
            }
            if (!/^(VALUE)?,(VALUE)?$/.test(type)) {
                throw new QueryParserInvalidSyntaxError();
            }
            if (type.startsWith('VALUE')) {
                result[1] = tokens[0].value;
                if (result[1] == null) {
                    throw new QueryParserInvalidSyntaxError();
                }
            }
            if (type.endsWith('VALUE')) {
                result[2] = tokens[l - 1].value;
                if (result[2] == null) {
                    throw new QueryParserInvalidSyntaxError();
                }
            }
            if (result[0] && result[1] == null) {
                throw new QueryParserInvalidSyntaxError();
            }
            if (result[3] && result[2] == null) {
                throw new QueryParserInvalidSyntaxError();
            }
            return result;
        };
        let filter = (token) => {
            if (token.type == 'RANGE_OPEN') {
                if (range) {
                    throw new QueryParserInvalidSyntaxError();
                }
                range = [];
                return false;
            }
            if (token.type == 'RANGE_CLOSE') {
                if (!range) {
                    throw new QueryParserInvalidSyntaxError();
                }
                token.type = 'RANGE';
                token.value = validator(range);
                range = null;
                return true;
            }
            if (!range) {
                if (token.type == ',') {
                    throw new QueryParserInvalidSyntaxError();
                }
                return true;
            }
            range.push(token);
            return false;
        };
        tokens = tokens.filter(filter);
        if (range) {
            throw new QueryParserInvalidSyntaxError();
        }
        return tokens;
    }

    normalizeMergePrefix(tokens) {
        let infix = this._tokens2infix(tokens);
        let replace = (allowPrefix, allowPostfix) => {
            return (m, val) => {
                val = +val;
                if (!utils.isObject(tokens[val].value)) {
                    tokens[val].value = {value: tokens[val].value};
                }
                if (allowPrefix) {
                    tokens[val - 1] = false;
                    tokens[val].value.allowPrefix = true;
                }
                if (allowPostfix) {
                    tokens[val + 1] = false;
                    tokens[val].value.allowPostfix = true;
                }
                return val + '-VALUE ';
            };
        };
        infix = infix.replace(/(\d+)-VALUE (\d+-ALL \d+-VALUE )+/g, (m) => {
            let ints = m.match(/\d+-/g).map((v) => parseInt(v));
            let vals = [];
            ints.forEach((int, i) => {
                if (i % 2 == 0) {
                    vals.push(tokens[int].value);
                }
            });
            let placeholders = ['*', '$', '-' + Number(new Date()) + '-'];
            let placeholder;
            while (!placeholder) {
                let v = placeholders.shift();
                if (!vals.some((val) => val.includes(v))) {
                    placeholder = v;
                }
            }
            let first = ints.shift();
            tokens[first].value = {placeholder, value: vals.join(placeholder)};
            ints.forEach((int) => tokens[int] = false);
            tokens = tokens.filter(Boolean);
            return first + '-VALUE ';
        });
        infix = infix.replace(/\d+-ALL (\d+)-VALUE /g, replace(true, null));
        infix = infix.replace(/(\d+)-VALUE \d+-ALL /g, replace(null, true));
        infix;
        tokens = tokens.filter(Boolean);
        for (let token of tokens) {
            if (token.type == 'ALL') {
                token.type = 'VALUE';
                token.value = null;
            }
        }
        return tokens;
    }

    normalizeMergeIdentCompare(tokens) {
        let inv = /(-FK_ALONE|-IDENT_ALONE|-RANGE|-VALUE) (-FK_ALONE|-IDENT_ALONE|-RANGE|-VALUE) /;
        let infix = this._tokens2infix(tokens, {prependIndex: false});
        if (inv.test(infix)) {
            // ex. "[1,2][3,4]", '"foo""bar"', "@a@@fk", "@a[1]"
            throw new QueryParserInvalidSyntaxError();
        }
        tokens = tokens.filter(({type}) => type != 'SPACE');
        infix = this._tokens2infix(tokens);
        if (infix.includes('( ) ')) {
            // ex. "()"
            throw new QueryParserInvalidSyntaxError();
        }
        let old;
        do {
            old = infix;
            infix = infix.replace(/\( (\d+-(VALUE|RANGE|FK_ALONE|IDENT_ALONE) )\) /g, (m, token) => {
                let i = parseInt(token);
                tokens[i - 1] = false;
                tokens[i + 1] = false;
                return token;
            });
        } while (old != infix);
        infix = infix.replace(/(\d+)-IDENT_COMPARE (\d+)-VALUE /g, (m, ident, value) => {
            let v = tokens[value].value;
            if (v == null || utils.isObject(v)) {
                throw new QueryParserInvalidSyntaxError();
            }
            let [field, cmp] = tokens[ident].value;
            tokens[ident] = {type: 'IDENT', value: field};
            let eq = cmp[1] == '=';
            cmp = cmp[0];
            tokens[value].value = cmp == '>' ? [!eq, v, null, false] : [false, null, v, !eq];
            return '';
        });
        tokens = tokens.filter(Boolean);
        if (/\d+-IDENT_COMPARE /.test(infix)) {
            throw new QueryParserInvalidSyntaxError();
        }
        for (let token of tokens) {
            if (token.type == 'RANGE') {
                token.type = 'VALUE';
            }
        }
        return tokens;
    }

    tokens2infix(tokens, terms) {
        utils.ok.instance(Terms, terms);
        let infix = this._tokens2infix(tokens, {appendPara: true});
        infix = infix.replace(/(\d+)-IDENT \(-(\d+) (.* )\)-\2 /g, (m, ident, op, content) => {
            ident = tokens[ident].value;
            if (/-(IDENT|FK|IDENT_ALONE|FK_ALONE) /.test(content)) {
                throw new QueryParserInvalidSyntaxError();
            }
            return '( ' + content.replace(/(\d+)-VALUE /g, (m, value) => {
                value = tokens[value].value;
                return terms.insert({field: ident, value}) + ' ';
            }) + ') ';
        });
        infix = infix.replace(/(\d+)-IDENT_ALONE /g, (m, ident) => {
            ident = tokens[ident].value;
            return terms.insert({field: ident, value: null}) + ' ';
        });
        infix = infix.replace(/(\d+)-FK_ALONE /g, (m, fk) => {
            fk = tokens[fk].value;
            return terms.insert({field: fk, infix: '1'}) + ' ';
        });
        infix = infix.replace(/(\d+)-IDENT (\d+)-VALUE /g, (m, ident, value) => {
            ident = tokens[ident].value;
            value = tokens[value].value;
            return terms.insert({field: ident, value}) + ' ';
        });
        infix = infix.replace(/(\d+)-VALUE /g, (m, value) => {
            value = tokens[value].value;
            return terms.insert({field: null, value}) + ' ';
        });
        let old;
        do {
            old = infix;
            infix = infix.replace(/(\d+)-FK (\d+) /g, (m, fk, term) => {
                fk = tokens[fk].value;
                return terms.insert({field: fk, infix: term}) + ' ';
            });
            for (let op of this.findForeignKeyPair(infix)) {
                let regex = new RegExp(`(\\d+)-FK \\(-${op} (.*) \\)-${op} `);
                infix = infix.replace(regex, (m, fk, content) => {
                    fk = tokens[fk].value;
                    return terms.insert({field: fk, infix: content}) + ' ';
                });
            }
        } while (old != infix);
        if (/-(IDENT|FK) /.test(infix)) {
            throw new QueryParserInvalidSyntaxError();
        }
        infix = infix.replace(/([()])-\d+ /g, '$1 ');
        infix = infix.replace(/(\d+|\)) ((- )*)(\d+|\() /g, '$1 & $2$4 ');
        return infix.trim();
    }

    _tokens2infix(tokens, options) {
        let para = 0;
        let level = [];
        let {
            prependIndex,
            appendPara,
        } = (options ?? {});
        prependIndex = prependIndex ?? true;
        appendPara = appendPara ?? false;
        let infix = tokens.map(({type, special}, i) => {
            if (appendPara && type == '(') {
                para++;
                level.push(para);
                return '(-' + para + ' ';
            }
            if (appendPara && type == ')') {
                let para = level.pop();
                if (!para) {
                    throw new QueryParserInvalidSyntaxError();
                }
                return ')-' + para + ' ';
            }
            return (special ? type : ((prependIndex ? i : '') + '-' + type)) + ' ';
        });
        if (level.length) {
            throw new QueryParserInvalidSyntaxError();
        }
        return infix.join('');
    }

    findForeignKeyPair(infix) {
        let ops = [];
        infix.replace(/-FK \(-(\d+) /g, (m, op) => ops.push(op));
        return ops.filter((op) => {
            let match, regex = new RegExp(`-FK \\(-${op} (.* )\\)-${op} `);
            infix.replace(regex, (m, content) => {
                match = !/-(FK|IDENT) /.test(content);
            });
            return match;
        });
    }

    infix2postfix(infix) {
        let output = [];
        let stack = [];
        let operators = {
            '-': 4,
            '&': 3,
            '|': 2,
        };
        infix = infix.replace(/\s+/g, '').split(/([-|&()])/).filter((v) => v != '');
        for (let token of infix) {
            if (!isNaN(parseInt(token))) {
                output.push(token);
                continue;
            }
            if (~'&|-'.indexOf(token)) {
                let o1 = token;
                let o2 = stack[stack.length - 1];
                while (~'&|-'.indexOf(o2) && operators[o1] <= operators[o2]) {
                    output.push(stack.pop());
                    o2 = stack[stack.length - 1];
                }
                stack.push(o1);
            } else if (token == '(') {
                stack.push(token);
            } else if (token == ')') {
                while (stack[stack.length - 1] !== '(') {
                    output.push(stack.pop());
                }
                stack.pop();
            }
        }
        while (stack.length > 0) {
            output.push(stack.pop());
        }
        let postfix = output.join(' ');
        return this.optimizePostfix(postfix);
    }

    optimizePostfix(postfix) {
        postfix = postfix.replace(/([&|])(?!\d+)/g, (m, p1) => p1 + '2');
        postfix += ' ';
        let old;
        do {
            old = postfix;
            postfix = postfix.replace(/(?<=^| )(-?\d+) - /g, (m, p1) => {
                if (p1 == '1') {
                    return '0 ';
                }
                if (p1 == '0') {
                    return '1 ';
                }
                return (-parseInt(p1)) + ' ';
            });
            postfix = postfix.replace(
                /([&|])(\d+) ((-?\d+ )*)\1(\d+) /g,
                (m, op, c1, terms, _, c2) => {
                    return terms + op + ((+c1) + (+c2) - 1) + ' ';
                },
            );
        } while (old != postfix);
        return postfix.trim();
    }
}

exports.QueryParser = QueryParser;

class QueryParserError extends Error {}

exports.QueryParserError = QueryParserError;

class QueryParserInvalidSyntaxError extends QueryParserError {}

exports.QueryParserInvalidSyntaxError = QueryParserInvalidSyntaxError;

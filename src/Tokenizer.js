const utils = require('ejz-utils');

class Tokenizer {
    constructor(rules) {
        utils.ok.object(rules);
        let modes = {};
        for (let [token, [regex, normalize, mode]] of Object.entries(rules)) {
            mode = mode ?? '';
            modes[mode] = modes[mode] || [];
            modes[mode].push([token, regex, normalize]);
        }
        this.modes = modes;
    }

    tokenize(string, options) {
        utils.ok.string(string);
        options = options ?? {};
        utils.ok.object(options);
        options.trim = options.trim ?? true;
        if (options.trim) {
            string = string.trim();
        }
        let tokens = [];
        let mode = '';
        w: while (string.length) {
            for (let [type, regex, normalize] of this.modes[mode]) {
                let match = string.match(regex);
                if (!match) {
                    continue;
                }
                let [prepend, value, newmode] = normalize(match);
                mode = newmode ?? mode;
                string = string.substring(match[0].length) + prepend;
                let token = {type};
                if (value != null) {
                    token.value = value;
                }
                tokens.push(token);
                continue w;
            }
            throw new TokenizerUnknownTokenError(string[0]);
        }
        if (mode) {
            throw new TokenizerModeError(mode);
        }
        return tokens;
    }
}

exports.Tokenizer = Tokenizer;

class TokenizerError extends Error {}

exports.TokenizerError = TokenizerError;

class TokenizerUnknownTokenError extends TokenizerError {}

exports.TokenizerUnknownTokenError = TokenizerUnknownTokenError;

class TokenizerModeError extends TokenizerError {}

exports.TokenizerModeError = TokenizerModeError;

const utils = require('ejz-utils');

class Terms {
    constructor() {
        // 0 - empty term, 1 - * term
        this.terms = [null, {}];
    }

    insert(term) {
        if (term == null) {
            return 0;
        }
        if (term.value == null && term.field == null) {
            return 1;
        }
        if (term.value == null) {
            delete term.value;
        }
        if (term.field == null) {
            delete term.field;
        }
        let idx = this.terms.findIndex((t) => utils.equals(term, t));
        if (~idx) {
            return idx;
        }
        let i = this.terms.length;
        this.terms.push(term);
        return i;
    }

    get(index) {
        if (index != null) {
            return this.terms[index];
        }
        return this.terms;
    }
}

exports.Terms = Terms;

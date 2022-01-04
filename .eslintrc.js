module.exports = {
    ignorePatterns: ['**/*.test.js'],
    env: {
        commonjs: true,
        es2021: true,
        node: true,
    },
    extends: [
        'eslint:recommended',
    ],
    rules: {
        semi: ['error', 'always'],
        quotes: ['error', 'single'],
        indent: ['error', 4, {SwitchCase: 1}],
    },
};

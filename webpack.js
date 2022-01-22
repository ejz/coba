const path = require('path');

const nodeExternals = require('webpack-node-externals');
const CopyPlugin = require('copy-webpack-plugin');
const TerserPlugin = require('terser-webpack-plugin');

const ROOT = path.resolve(__dirname);
const DIST = path.resolve(ROOT, 'dist');
const SRC = path.resolve(ROOT, 'src');
const ETC = path.resolve(ROOT, 'etc');

module.exports = {
    target: 'node',
    entry: {
        di: path.resolve(SRC, 'di'),
    },
    externals: [nodeExternals()],
    mode: 'production',
    resolve: {
        extensions: ['.js'],
        modules: ['node_modules'],
    },
    output: {
        filename: '[name].js',
        path: DIST,
        library: {
            type: 'commonjs',
        },
    },
    plugins: [
        new CopyPlugin({
            patterns: [
                {
                    from: SRC,
                    to: DIST,
                    filter: (f) => f.endsWith('.proto'),
                },
                {
                    from: ETC,
                    to: DIST,
                    filter: (f) => f.endsWith('.ini'),
                },
                {
                    from: ROOT,
                    to: DIST,
                    filter: (f) => /\/bin\.js$/.test(f),
                },
            ],
        }),
    ],
    optimization: {
        splitChunks: {
            automaticNameDelimiter: '_',
        },
        minimize: true,
        minimizer: [
            new TerserPlugin({
                terserOptions: {
                    output: {
                        comments: false,
                    },
                    mangle: {
                        keep_classnames: /Error$/,
                    },
                },
                extractComments: false,
            }),
        ],
    },
};

const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname);
const PACKAGE = path.resolve(ROOT, '..', 'package.json');
const TARGET = path.resolve(ROOT, '..', 'dist', 'package.json');

let package = JSON.parse(fs.readFileSync(PACKAGE));

let ver = package.version;

// PATCH
ver = ver.split('.');
ver[ver.length - 1] = parseInt(ver[ver.length - 1]) + 1;
ver = ver.join('.');

// MINOR @TODO

package.version = ver;

fs.writeFileSync(PACKAGE, JSON.stringify(package, null, 4) + '\n');

package = Object.fromEntries(Object.entries(package).filter(([k]) => {
    return [
        'name',
        'version',
        'description',
        'main',
        'bin',
        'keywords',
        'author',
        'license',
        'dependencies',
        'repository',
    ].includes(k);
}));

fs.writeFileSync(TARGET, JSON.stringify(package));

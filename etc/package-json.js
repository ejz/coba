const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname);
const PACKAGE = path.resolve(ROOT, '..', 'package.json');
const TARGET = path.resolve(ROOT, '..', 'dist', 'package.json');

let package = JSON.parse(fs.readFileSync(PACKAGE));

let ver = package.version;

// DO NOTHING WITH VERSION
// VERSION IS INCREASED MANUALLY

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

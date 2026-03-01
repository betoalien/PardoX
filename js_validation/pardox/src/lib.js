'use strict';

const os   = require('os');
const path = require('path');
const fs   = require('fs');

/**
 * Resolves the absolute path to the PardoX native binary based on the
 * current operating system and CPU architecture.
 *
 * Mirrors Python's wrapper.py and PHP's Core/Lib.php.
 *
 * Expected layout (relative to this file):
 *   ../libs/Linux/pardox-cpu-Linux-x64.so
 *   ../libs/Win/pardox-cpu-Windows-x64.dll
 *   ../libs/Mac/pardox-cpu-MacOS-ARM64.dylib
 *   ../libs/Mac/pardox-cpu-MacOS-Intel.dylib
 *
 * @returns {string} Absolute path to the shared library.
 * @throws  {Error}  If the OS/architecture is unsupported or the file is missing.
 */
function getLibraryPath() {
    const platform = os.platform();  // 'linux', 'win32', 'darwin'
    const arch     = os.arch();      // 'x64', 'arm64', etc.

    const libsBase = path.join(__dirname, '..', 'libs');

    let libName;
    let folder;

    if (platform === 'linux') {
        libName = 'pardox-cpu-Linux-x64.so';
        folder  = 'Linux';
    } else if (platform === 'win32') {
        libName = 'pardox-cpu-Windows-x64.dll';
        folder  = 'Win';
    } else if (platform === 'darwin') {
        folder = 'Mac';
        if (arch === 'arm64') {
            libName = 'pardox-cpu-MacOS-ARM64.dylib';
        } else if (arch === 'x64') {
            libName = 'pardox-cpu-MacOS-Intel.dylib';
        } else {
            throw new Error(`Unsupported macOS architecture: ${arch}`);
        }
    } else {
        throw new Error(`Unsupported operating system: ${platform}`);
    }

    const libPath = path.join(libsBase, folder, libName);

    if (!fs.existsSync(libPath)) {
        throw new Error(
            `PardoX binary not found.\nExpected at: ${libPath}\n` +
            `Ensure the 'libs/' folder contains the correct binaries for your OS.`
        );
    }

    return libPath;
}

module.exports = { getLibraryPath };

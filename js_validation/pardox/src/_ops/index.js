'use strict';

const visualization            = require('./visualization');
const { defineMetadata }       = require('./metadata');
const selection                = require('./selection');
const mutation                 = require('./mutation');
const writers                  = require('./writers');
const exportOps                = require('./export');
const mathOps                  = require('./math_ops');
const gpu                      = require('./gpu');
const reshape                  = require('./reshape');
const timeseries               = require('./timeseries');
const nested                   = require('./nested');
const spill                    = require('./spill');
const groupby                  = require('./groupby');
const strings                  = require('./strings');
const datetime                 = require('./datetime');
const decimal                  = require('./decimal');
const window                   = require('./window');
const sql                      = require('./sql');
const encryption               = require('./encryption');
const contracts                = require('./contracts');
const timetravel               = require('./timetravel');
const cluster                  = require('./cluster');
const linalg                   = require('./linalg');

/**
 * Mix all instance methods into DataFrame.prototype.
 * Static methods (spillFromDisk, memoryUsage, contractViolationCount) are NOT
 * included here — they are applied in DataFrame.js directly onto the constructor.
 *
 * @param {Object} proto  DataFrame.prototype
 */
function applyAll(proto) {
    // Getters must be installed via defineProperty, not Object.assign,
    // to avoid invoking them immediately during mixin.
    defineMetadata(proto);

    // Pull static_spillFromDisk out before merging so it doesn't pollute the prototype
    const { static_spillFromDisk, ...spillInstance } = spill;

    Object.assign(proto,
        visualization,
        selection,
        mutation,
        writers,
        exportOps,
        mathOps,
        gpu,
        reshape,
        timeseries,
        nested,
        spillInstance,
        groupby,
        strings,
        datetime,
        decimal,
        window,
        sql,
        encryption,
        contracts,
        timetravel,
        cluster,
        linalg
    );
}

module.exports = { applyAll, spill };

'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Fetch JSON data from a REST API endpoint into a DataFrame.
 * @param {string} url      REST endpoint URL.
 * @param {string} method   HTTP method (default 'GET').
 * @param {Object} headers  Optional HTTP headers.
 * @returns {DataFrame}
 */
function readRest(url, method = 'GET', headers = {}) {
    const lib = getLib();
    const ptr = lib.pardox_read_rest(url, method, JSON.stringify(headers));
    if (!ptr) throw new Error(`readRest failed for URL: ${url}`);
    return new DataFrame(ptr);
}

module.exports = { readRest };

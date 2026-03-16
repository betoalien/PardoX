'use strict';

const { getLib } = require('../ffi');
const { DataFrame } = require('../DataFrame');

/**
 * Read a CSV file from cloud storage (S3/GCS/Azure) into a DataFrame.
 * @param {string} url          Cloud URL (e.g. "s3://bucket/file.csv").
 * @param {Object} schema       Optional schema override.
 * @param {Object} config       Optional CSV config.
 * @param {Object} credentials  Cloud credentials.
 * @returns {DataFrame}
 */
function readCloudCsv(url, schema = {}, config = {}, credentials = {}) {
    const lib = getLib();
    const ptr = lib.pardox_cloud_read_csv(
        url,
        JSON.stringify(schema),
        JSON.stringify(config),
        JSON.stringify(credentials)
    );
    if (!ptr) throw new Error(`readCloudCsv failed for: ${url}`);
    return new DataFrame(ptr);
}

/**
 * Read a .prdx file from cloud storage into a DataFrame.
 * @param {string} url          Cloud URL.
 * @param {number} limit        Maximum rows to read.
 * @param {Object} credentials  Cloud credentials.
 * @returns {DataFrame}
 */
function readCloudPrdx(url, limit = -1, credentials = {}) {
    const lib = getLib();
    const ptr = lib.pardox_cloud_read_prdx(url, limit, JSON.stringify(credentials));
    if (!ptr) throw new Error(`readCloudPrdx failed for: ${url}`);
    return new DataFrame(ptr);
}

/**
 * Write a DataFrame as a .prdx file to cloud storage.
 * @param {DataFrame} df           DataFrame to write.
 * @param {string}    url          Destination cloud URL.
 * @param {Object}    credentials  Cloud credentials.
 * @returns {number} Bytes written.
 */
function writeCloudPrdx(df, url, credentials = {}) {
    const lib    = getLib();
    const result = lib.pardox_cloud_write_prdx(df._ptr, url, JSON.stringify(credentials));
    if (result < 0) throw new Error(`writeCloudPrdx failed with code: ${result}`);
    return result;
}

module.exports = { readCloudCsv, readCloudPrdx, writeCloudPrdx };

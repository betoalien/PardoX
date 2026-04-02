'use strict';

/**
 * PardoX v0.3.2 - Gap 11: Spill to Disk
 * Validates spill_to_disk, spill_from_disk, chunked_groupby, external_sort,
 * and memory_usage operations via the JavaScript SDK.
 *
 * Tests:
 *  1. spill_to_disk - write DataFrame to disk
 *  2. spill_from_disk - read spilled DataFrame
 *  3. spill roundtrip - data preserved after spill/unspill
 *  4. spill_to_disk - returns success code
 *  5. spill_from_disk - preserves schema
 *  6. spill_from_disk - preserves row count
 *  7. chunked_groupby - basic chunked aggregation
 *  8. chunked_groupby - sum aggregation
 *  9. chunked_groupby - count aggregation
 * 10. chunked_groupby - mean aggregation
 * 11. chunked_groupby - preserves group keys
 * 12. chunked_groupby - chunk size parameter respected
 * 13. external_sort - sort large dataset
 * 14. external_sort - ascending order
 * 15. external_sort - descending order
 * 16. external_sort - preserves all columns
 * 17. external_sort - chunk size parameter
 * 18. memory_usage - returns positive number
 * 19. memory_usage - increases with more data
 * 20. memory_usage - same DataFrame same memory
 * 21. spill_to_disk - invalid path throws
 * 22. spill_from_disk - non-existent file returns null
 * 23. chunked_groupby - invalid column throws
 * 24. external_sort - invalid column throws
 * 25. spill_to_disk - large DataFrame (10K rows)
 * 26. chunked_groupby - result has fewer rows than input
 * 27. external_sort - sorted values in order
 * 28. memory_usage - returns bytes (reasonable range)
 * 29. spill_from_disk - multiple reads same file
 * 30. chunked_groupby - multiple aggregations
 */

const { DataFrame } = require('@pardox/pardox');
const fs = require('fs');

const SPILL_PATH = 'spill_test.prdx';
const SPILL_PATH_LARGE = 'spill_large.prdx';

let passed = 0;
let failed = 0;
const results = [];

function section(title) {
    console.log(`\n${'='.repeat(66)}`);
    console.log(`  ${title}`);
    console.log('='.repeat(66));
}

function check(name, condition, detail = '') {
    const status = condition ? 'PASS' : 'FAIL';
    results.push({ name, status, detail });
    const mark = condition ? '[OK]' : '[FAIL]';
    console.log(`  ${mark} ${name}${detail ? '  - ' + detail : ''}`);
    if (condition) passed++;
    else failed++;
}

console.log('\n=== Gap 11: Spill to Disk (JS SDK) ===\n');

// Create test DataFrame
const testData = [];
for (let i = 0; i < 100; i++) {
    testData.push({
        id: i,
        group: i % 5,  // 5 groups: 0, 1, 2, 3, 4
        value: Math.random() * 100,
        category: ['A', 'B', 'C'][i % 3]
    });
}
const df = DataFrame.fromArray(testData);

// ===========================================================================
// SPILL_TO_DISK TESTS
// ===========================================================================
section('Spill To Disk Tests');

// Test 1: spill_to_disk write DataFrame to disk
let spillSuccess = false;
try {
    const result = df.spillToDisk(SPILL_PATH);
    spillSuccess = result > 0;  // Returns bytes written, not 1
    check('spill_to_disk writes file', spillSuccess && fs.existsSync(SPILL_PATH), 'file created');
} catch (e) {
    check('spill_to_disk writes file', false, e.message);
}

// Test 4: spill_to_disk returns bytes written (> 0)
try {
    const result = df.spillToDisk(SPILL_PATH);
    check('spill_to_disk returns bytes', result > 0, `returned ${result} bytes`);
} catch (e) {
    check('spill_to_disk returns bytes', false, e.message);
}

// Test 21: spill_to_disk invalid path throws
let spillError = false;
try {
    df.spillToDisk('nonexistent.prdx');
} catch (e) {
    spillError = true;
}
check('spill_to_disk invalid path throws', spillError, 'error thrown as expected');

// Test 25: spill_to_disk large DataFrame (10K rows)
try {
    const largeData = [];
    for (let i = 0; i < 10000; i++) {
        largeData.push({
            id: i,
            value: Math.random() * 1000,
            category: ['A', 'B', 'C', 'D', 'E'][i % 5]
        });
    }
    const dfLarge = DataFrame.fromArray(largeData);
    const result = dfLarge.spillToDisk(SPILL_PATH_LARGE);
    const fileExists = fs.existsSync(SPILL_PATH_LARGE);
    check('spill_to_disk large DataFrame', result > 0 && fileExists, '10K rows spilled');
    fs.unlinkSync(SPILL_PATH_LARGE);
} catch (e) {
    check('spill_to_disk large DataFrame', false, e.message);
}

// ===========================================================================
// SPILL_FROM_DISK TESTS
// ===========================================================================
section('Spill From Disk Tests');

// Test 2: spill_from_disk read spilled DataFrame
let loadedDf = null;
try {
    if (fs.existsSync(SPILL_PATH)) {
        loadedDf = DataFrame.spillFromDisk(SPILL_PATH);
        check('spill_from_disk reads file', loadedDf !== null, 'DataFrame loaded');
    } else {
        check('spill_from_disk reads file', false, 'spill file not found');
    }
} catch (e) {
    check('spill_from_disk reads file', false, e.message);
}

// Test 3: spill roundtrip data preserved
try {
    if (loadedDf) {
        const originalJson = df.toDict();
        const loadedJson = loadedDf.toDict();
        const sameRowCount = originalJson.length === loadedJson.length;
        check('spill roundtrip row count', sameRowCount,
            `original=${originalJson.length} loaded=${loadedJson.length}`);
    } else {
        check('spill roundtrip row count', false, 'no loaded DataFrame');
    }
} catch (e) {
    check('spill roundtrip row count', false, e.message);
}

// Test 5: spill_from_disk preserves schema
try {
    if (loadedDf) {
        const originalCols = df.columns.sort().join(',');
        const loadedCols = loadedDf.columns.sort().join(',');
        const sameSchema = originalCols === loadedCols;
        check('spill_from_disk preserves schema', sameSchema,
            `cols: ${loadedCols}`);
    } else {
        check('spill_from_disk preserves schema', false, 'no loaded DataFrame');
    }
} catch (e) {
    check('spill_from_disk preserves schema', false, e.message);
}

// Test 6: spill_from_disk preserves row count
try {
    if (loadedDf) {
        const originalRows = df.shape[0];
        const loadedRows = loadedDf.shape[0];
        const sameRows = originalRows === loadedRows;
        check('spill_from_disk preserves rows', sameRows,
            `original=${originalRows} loaded=${loadedRows}`);
    } else {
        check('spill_from_disk preserves rows', false, 'no loaded DataFrame');
    }
} catch (e) {
    check('spill_from_disk preserves rows', false, e.message);
}

// Test 22: spill_from_disk non-existent file returns null
let nonExistentNull = false;
try {
    const result = DataFrame.spillFromDisk('nonexistent.prdx');
    nonExistentNull = result === null;
} catch (e) {
    nonExistentNull = true;  // Throwing is also acceptable
}
check('spill_from_disk non-existent file', nonExistentNull, 'null or exception');

// Test 29: spill_from_disk multiple reads same file
try {
    if (fs.existsSync(SPILL_PATH)) {
        const df1 = DataFrame.spillFromDisk(SPILL_PATH);
        const df2 = DataFrame.spillFromDisk(SPILL_PATH);
        const bothValid = df1 !== null && df2 !== null;
        const sameRows = df1.shape[0] === df2.shape[0];
        check('spill_from_disk multiple reads', bothValid && sameRows, 'both reads successful');
    } else {
        check('spill_from_disk multiple reads', false, 'file not found');
    }
} catch (e) {
    check('spill_from_disk multiple reads', false, e.message);
}

// ===========================================================================
// CHUNKED_GROUPBY TESTS
// ===========================================================================
section('Chunked GroupBy Tests');

// Test 7: chunked_groupby basic chunked aggregation
try {
    const result = df.chunkedGroupby('group', { value: 'sum' }, 50);
    const json = result.toDict();
    check('chunked_groupby basic', json.length > 0, `${json.length} groups`);
} catch (e) {
    check('chunked_groupby basic', false, e.message);
}

// Test 8: chunked_groupby sum aggregation
try {
    const result = df.chunkedGroupby('group', { value: 'sum' }, 50);
    const json = result.toDict();
    // Should have 5 groups (0-4)
    const hasFiveGroups = json.length === 5;
    check('chunked_groupby sum', hasFiveGroups, `${json.length} groups (expected 5)`);
} catch (e) {
    check('chunked_groupby sum', false, e.message);
}

// Test 9: chunked_groupby count aggregation
try {
    const result = df.chunkedGroupby('group', { value: 'count' }, 50);
    const json = result.toDict();
    const hasFiveGroups = json.length === 5;
    // Each group should have ~20 rows (100 / 5)
    const counts = json.map(r => r.value);
    const allTwenty = counts.every(c => Math.abs(c - 20) <= 1);
    check('chunked_groupby count', hasFiveGroups && allTwenty, `counts: ${counts.join(', ')}`);
} catch (e) {
    check('chunked_groupby count', false, e.message);
}

// Test 10: chunked_groupby mean aggregation
try {
    const result = df.chunkedGroupby('group', { value: 'mean' }, 50);
    const json = result.toDict();
    const hasFiveGroups = json.length === 5;
    // Mean values should be in reasonable range (0-100)
    const means = json.map(r => r.value);
    const inRange = means.every(m => m >= 0 && m <= 100);
    check('chunked_groupby mean', hasFiveGroups && inRange, `means in range [0,100]`);
} catch (e) {
    check('chunked_groupby mean', false, e.message);
}

// Test 11: chunked_groupby preserves group keys
try {
    const result = df.chunkedGroupby('group', { value: 'sum' }, 50);
    const json = result.toDict();
    const groups = json.map(r => r.group);
    const hasAllGroups = [0, 1, 2, 3, 4].every(g => groups.includes(g));
    check('chunked_groupby preserves keys', hasAllGroups, `groups: ${groups.join(', ')}`);
} catch (e) {
    check('chunked_groupby preserves keys', false, e.message);
}

// Test 12: chunked_groupby chunk size parameter respected
try {
    // Use different chunk sizes and verify both work
    const result1 = df.chunkedGroupby('group', { value: 'sum' }, 10);
    const result2 = df.chunkedGroupby('group', { value: 'sum' }, 100);
    const json1 = result1.toDict();
    const json2 = result2.toDict();
    // Results should be similar regardless of chunk size
    const sameGroups = json1.length === json2.length;
    check('chunked_groupby chunk size', sameGroups,
        `chunk=10: ${json1.length} groups, chunk=100: ${json2.length} groups`);
} catch (e) {
    check('chunked_groupby chunk size', false, e.message);
}

// Test 23: chunked_groupby invalid column throws
let chunkedError = false;
try {
    df.chunkedGroupby('nonexistent', { value: 'sum' }, 50);
} catch (e) {
    chunkedError = true;
}
check('chunked_groupby invalid column throws', chunkedError, 'error thrown as expected');

// Test 26: chunked_groupby result has fewer rows than input
try {
    const result = df.chunkedGroupby('group', { value: 'sum' }, 50);
    const fewerRows = result.shape[0] < df.shape[0];
    check('chunked_groupby fewer rows', fewerRows,
        `original=${df.shape[0]} result=${result.shape[0]}`);
} catch (e) {
    check('chunked_groupby fewer rows', false, e.message);
}

// Test 30: chunked_groupby multiple aggregations
try {
    const result = df.chunkedGroupby('group', { value: 'sum', id: 'count' }, 50);
    const json = result.toDict();
    const hasBoth = json.every(r => 'value' in r && 'id' in r);
    check('chunked_groupby multiple aggs', hasBoth, 'both aggregations present');
} catch (e) {
    check('chunked_groupby multiple aggs', false, e.message);
}

// ===========================================================================
// EXTERNAL_SORT TESTS
// ===========================================================================
section('External Sort Tests');

// Test 13: external_sort sort large dataset
try {
    const result = df.externalSort('value', true, 50);
    const json = result.toDict();
    check('external_sort large dataset', json.length === df.shape[0],
        `${json.length} rows sorted`);
} catch (e) {
    check('external_sort large dataset', false, e.message);
}

// Test 14: external_sort ascending order
try {
    const result = df.externalSort('value', true, 50);
    const json = result.toDict();
    const values = json.map(r => r.value);
    let sorted = true;
    for (let i = 1; i < values.length; i++) {
        if (values[i] < values[i-1]) {
            sorted = false;
            break;
        }
    }
    check('external_sort ascending', sorted, 'values in ascending order');
} catch (e) {
    check('external_sort ascending', false, e.message);
}

// Test 15: external_sort descending order
try {
    const result = df.externalSort('value', false, 50);
    const json = result.toDict();
    const values = json.map(r => r.value);
    let sorted = true;
    for (let i = 1; i < values.length; i++) {
        if (values[i] > values[i-1]) {
            sorted = false;
            break;
        }
    }
    check('external_sort descending', sorted, 'values in descending order');
} catch (e) {
    check('external_sort descending', false, e.message);
}

// Test 16: external_sort preserves all columns
try {
    const result = df.externalSort('value', true, 50);
    const originalCols = df.columns.sort().join(',');
    const resultCols = result.columns.sort().join(',');
    const sameCols = originalCols === resultCols;
    check('external_sort preserves columns', sameCols, `cols: ${resultCols}`);
} catch (e) {
    check('external_sort preserves columns', false, e.message);
}

// Test 17: external_sort chunk size parameter
try {
    // Use different chunk sizes
    const result1 = df.externalSort('value', true, 10);
    const result2 = df.externalSort('value', true, 100);
    const json1 = result1.toDict();
    const json2 = result2.toDict();
    // Both should be sorted correctly
    const values1 = json1.map(r => r.value);
    const values2 = json2.map(r => r.value);
    const sorted1 = values1.every((v, i) => i === 0 || v >= values1[i-1]);
    const sorted2 = values2.every((v, i) => i === 0 || v >= values2[i-1]);
    check('external_sort chunk size', sorted1 && sorted2, 'both chunk sizes work');
} catch (e) {
    check('external_sort chunk size', false, e.message);
}

// Test 24: external_sort invalid column throws
let externalError = false;
try {
    df.externalSort('nonexistent', true, 50);
} catch (e) {
    externalError = true;
}
check('external_sort invalid column throws', externalError, 'error thrown as expected');

// Test 27: external_sort sorted values in order
try {
    // Create small dataset for exact verification
    const smallData = [{id: 1, val: 30}, {id: 2, val: 10}, {id: 3, val: 20}];
    const dfSmall = DataFrame.fromArray(smallData);
    const result = dfSmall.externalSort('val', true, 10);
    const json = result.toDict();
    const values = json.map(r => r.val);
    const correctOrder = values[0] === 10 && values[1] === 20 && values[2] === 30;
    check('external_sort correct order', correctOrder,
        `order: [${values.join(', ')}]`);
} catch (e) {
    check('external_sort correct order', false, e.message);
}

// ===========================================================================
// MEMORY_USAGE TESTS
// ===========================================================================
section('Memory Usage Tests');

// Test 18: memory_usage returns positive number
try {
    const memUsage = df.memoryUsage();
    check('memory_usage positive', memUsage > 0, `${memUsage} bytes`);
} catch (e) {
    check('memory_usage positive', false, e.message);
}

// Test 19: memory_usage increases with more data
try {
    const smallData = [{id: 1, val: 10}];
    const dfSmall = DataFrame.fromArray(smallData);
    const memSmall = dfSmall.memoryUsage();
    const memLarge = df.memoryUsage();
    const increases = memLarge > memSmall;
    check('memory_usage increases with data', increases,
        `small=${memSmall} bytes, large=${memLarge} bytes`);
} catch (e) {
    check('memory_usage increases with data', false, e.message);
}

// Test 20: memory_usage same DataFrame same memory
try {
    const mem1 = df.memoryUsage();
    const mem2 = df.memoryUsage();
    const same = mem1 === mem2;
    check('memory_usage consistent', same, `both: ${mem1} bytes`);
} catch (e) {
    check('memory_usage consistent', false, e.message);
}

// Test 28: memory_usage returns bytes (reasonable range)
try {
    const memUsage = df.memoryUsage();
    // 100 rows with 4 columns should be at least a few KB
    const reasonable = memUsage > 100 && memUsage < 100 * 1024 * 1024;  // Between 100B and 100MB
    check('memory_usage reasonable range', reasonable, `${memUsage} bytes`);
} catch (e) {
    check('memory_usage reasonable range', false, e.message);
}

// -- Cleanup temp files --
try { fs.unlinkSync(SPILL_PATH); } catch (e) {}
try { fs.unlinkSync(SPILL_PATH_LARGE); } catch (e) {}

// ===========================================================================
// Summary
// ===========================================================================
section('FINAL RESULT');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 11 Spill to Disk VALIDATED');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}

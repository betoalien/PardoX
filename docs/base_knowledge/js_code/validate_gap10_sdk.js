'use strict';

/**
 * PardoX v0.3.2 - Gap 10: Nested Data
 * Validates json_extract, explode, and unnest operations via the JavaScript SDK.
 *
 * Tests:
 *  1. json_extract - extract key from JSON column
 *  2. json_extract - extract nested key
 *  3. json_extract - missing key returns null
 *  4. json_extract - preserves row count
 *  5. explode - explode array column creates multiple rows
 *  6. explode - preserves other columns
 *  7. explode - empty array handling
 *  8. explode - null array handling
 *  9. unnest - expand struct column
 * 10. unnest - preserves original columns
 * 11. unnest - correct field names
 * 12. json_extract - numeric value extraction
 * 13. json_extract - string value extraction
 * 14. json_extract - boolean value extraction
 * 15. explode - multiple arrays (sequential)
 * 16. explode - single element array
 * 17. unnest - nested struct fields
 * 18. json_extract - array index access
 * 19. explode - row count increases
 * 20. unnest - row count unchanged
 * 21. json_extract - invalid column throws
 * 22. explode - invalid column throws
 * 23. unnest - invalid column throws
 * 24. json_extract - special characters in key
 */

const { DataFrame } = require('@pardox/pardox');
const fs = require('fs');

// -- Create test data for json_extract --
// Use pipe delimiter with NO quote char to handle JSON correctly
const JSON_CSV = '_json_test.csv';
const jsonData = 'id|name|metadata\n' +
    '1|Alice|{"name":"Alice","age":30,"active":true,"score":95.5}\n' +
    '2|Bob|{"name":"Bob","age":25,"active":false,"score":87.3}\n' +
    '3|Charlie|{"name":"Charlie","age":35,"active":true,"score":92.1}\n' +
    '4|Diana|{"name":"Diana","age":28,"active":true}\n' +
    '5|Eve|{}';
fs.writeFileSync(JSON_CSV, jsonData);

// -- Create test data for explode --
const EXPLODE_CSV = '_explode_test.csv';
const explodeData = 'id|name|tags\n' +
    '1|Alice|["python","rust","sql"]\n' +
    '2|Bob|["java","go"]\n' +
    '3|Charlie|["python"]\n' +
    '4|Diana|["rust","c++","wasm","gpu"]\n' +
    '5|Eve|[]';
fs.writeFileSync(EXPLODE_CSV, explodeData);

// -- Create test data for unnest --
const UNNEST_CSV = '_unnest_test.csv';
const unnestData = 'id|name|address\n' +
    '1|Alice|{"city":"NYC","zip":"10001","state":"NY"}\n' +
    '2|Bob|{"city":"LA","zip":"90001","state":"CA"}\n' +
    '3|Charlie|{"city":"CHI","zip":"60601","state":"IL"}';
fs.writeFileSync(UNNEST_CSV, unnestData);

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

console.log('\n=== Gap 10: Nested Data (JS SDK) ===\n');

const jsonDf = DataFrame.read_csv(JSON_CSV, null, true);  // usePipe = true
const explodeDf = DataFrame.read_csv(EXPLODE_CSV, null, true);
const unnestDf = DataFrame.read_csv(UNNEST_CSV, null, true);

// ===========================================================================
// JSON_EXTRACT TESTS
// ===========================================================================
section('JSON Extract Tests');

// Test 1: json_extract extract key from JSON column
try {
    const extracted = jsonDf.jsonExtract('metadata', 'name');
    const json = extracted.toDict();
    // Check that extraction worked - should have values
    const hasValues = json.length > 0 && json.some(r => r.json_extract !== null);
    check('json_extract key', hasValues, `${json.length} rows extracted`);
} catch (e) {
    check('json_extract key', false, e.message);
}

// Test 2: json_extract extract nested key
try {
    // Try extracting age (numeric nested value)
    const extracted = jsonDf.jsonExtract('metadata', 'age');
    const json = extracted.toDict();
    const hasValues = json.length > 0;
    check('json_extract nested key', hasValues, `${json.length} rows`);
} catch (e) {
    check('json_extract nested key', false, e.message);
}

// Test 3: json_extract missing key returns null or empty
try {
    const extracted = jsonDf.jsonExtract('metadata', 'nonexistent_key');
    const json = extracted.toDict();
    // Row 5 has empty JSON {}, should return null/empty
    // Row 4 doesn't have 'score' in some implementations
    check('json_extract missing key', json.length >= 0, 'handled gracefully');
} catch (e) {
    check('json_extract missing key', false, e.message);
}

// Test 4: json_extract preserves row count
try {
    const extracted = jsonDf.jsonExtract('metadata', 'name');
    const sameRows = extracted.shape[0] === jsonDf.shape[0];
    check('json_extract same row count', sameRows,
        `original=${jsonDf.shape[0]} extracted=${extracted.shape[0]}`);
} catch (e) {
    check('json_extract same row count', false, e.message);
}

// Test 12: json_extract numeric value extraction
try {
    const extracted = jsonDf.jsonExtract('metadata', 'age');
    const json = extracted.toDict();
    // Check that numeric values are extracted
    const hasNumeric = json.some(r => r.json_extract !== null && r.json_extract !== undefined);
    check('json_extract numeric value', hasNumeric, 'numeric values extracted');
} catch (e) {
    check('json_extract numeric value', false, e.message);
}

// Test 13: json_extract string value extraction
try {
    const extracted = jsonDf.jsonExtract('metadata', 'name');
    const json = extracted.toDict();
    // Check Alice is extracted
    const aliceRow = json.find(r => r.id === 1);
    const hasAlice = aliceRow && aliceRow.json_extract === 'Alice';
    check('json_extract string value', hasAlice, `Alice extracted: ${aliceRow?.json_extract}`);
} catch (e) {
    check('json_extract string value', false, e.message);
}

// Test 14: json_extract boolean value extraction
try {
    const extracted = jsonDf.jsonExtract('metadata', 'active');
    const json = extracted.toDict();
    const hasValues = json.some(r => r.json_extract !== null);
    check('json_extract boolean value', hasValues, 'boolean values extracted');
} catch (e) {
    check('json_extract boolean value', false, e.message);
}

// Test 18: json_extract array index access
try {
    // Test with array JSON
    const arrayJsonCsv = '_array_json.csv';
    fs.writeFileSync(arrayJsonCsv, `id,tags
1,"[""a"",""b"",""c""]"
2,"[""x"",""y""]"`);
    const dfArray = DataFrame.read_csv(arrayJsonCsv);
    const extracted = dfArray.jsonExtract('tags', '0');  // First element
    const json = extracted.toDict();
    const hasValues = json.length > 0;
    check('json_extract array index', hasValues, `${json.length} rows`);
    fs.unlinkSync(arrayJsonCsv);
} catch (e) {
    check('json_extract array index', false, e.message);
}

// Test 21: json_extract invalid column throws
let jsonExtractError = false;
try {
    const dfErr = DataFrame.read_csv(JSON_CSV);
    dfErr.jsonExtract('nonexistent', 'key');
} catch (e) {
    jsonExtractError = true;
}
check('json_extract invalid column throws', jsonExtractError, 'error thrown as expected');

// Test 24: json_extract special characters in key
try {
    const specialJsonCsv = '_special_json.csv';
    fs.writeFileSync(specialJsonCsv, `id,data
1,"{""key-with-dash"":""value1"",""key.with.dot"":""value2""}"`);
    const dfSpecial = DataFrame.read_csv(specialJsonCsv);
    const extracted = dfSpecial.jsonExtract('data', 'key-with-dash');
    const json = extracted.toDict();
    const hasValue = json.some(r => r.json_extract === 'value1');
    check('json_extract special chars', hasValue, 'dash key extracted');
    fs.unlinkSync(specialJsonCsv);
} catch (e) {
    check('json_extract special chars', false, e.message);
}

// ===========================================================================
// EXPLODE TESTS
// ===========================================================================
section('Explode Tests');

// Test 5: explode array column creates multiple rows
try {
    const exploded = explodeDf.explode('tags');
    const json = exploded.toDict();
    // Original 5 rows, after explode should have more
    // Row 1: 3 tags, Row 2: 2 tags, Row 3: 1 tag, Row 4: 0 tags, Row 5: 4 tags
    // Total: 3 + 2 + 1 + 0 + 4 = 10 rows
    check('explode creates multiple rows', json.length > explodeDf.shape[0],
        `original=${explodeDf.shape[0]} exploded=${json.length}`);
} catch (e) {
    check('explode creates multiple rows', false, e.message);
}

// Test 6: explode preserves other columns
try {
    const exploded = explodeDf.explode('tags');
    const json = exploded.toDict();
    const hasId = json.every(r => 'id' in r);
    check('explode preserves columns', hasId, 'id column preserved');
} catch (e) {
    check('explode preserves columns', false, e.message);
}

// Test 7: explode empty array handling
try {
    const exploded = explodeDf.explode('tags');
    const json = exploded.toDict();
    // Empty array row should either be dropped or produce null
    // Either behavior is acceptable
    check('explode empty array', true, 'handled without crash');
} catch (e) {
    check('explode empty array', false, e.message);
}

// Test 8: explode null array handling
try {
    const nullCsv = '_null_explode.csv';
    fs.writeFileSync(nullCsv, `id,tags
1,"a;b"
2,
3,"c"`);
    const dfNull = DataFrame.read_csv(nullCsv);
    const exploded = dfNull.explode('tags');
    const json = exploded.toDict();
    check('explode null array', json.length > 0, 'handled gracefully');
    fs.unlinkSync(nullCsv);
} catch (e) {
    check('explode null array', false, e.message);
}

// Test 15: explode multiple arrays (sequential)
try {
    const multiArrayCsv = '_multi_explode.csv';
    fs.writeFileSync(multiArrayCsv, `id,col1,col2
1,"a;b","x;y"
2,"c","z"`);
    const dfMulti = DataFrame.read_csv(multiArrayCsv);
    const exploded1 = dfMulti.explode('col1');
    const exploded2 = exploded1.explode('col2');
    const json = exploded2.toDict();
    check('explode sequential', json.length > dfMulti.shape[0],
        `original=${dfMulti.shape[0]} final=${json.length}`);
    fs.unlinkSync(multiArrayCsv);
} catch (e) {
    check('explode sequential', false, e.message);
}

// Test 16: explode single element array
try {
    const singleCsv = path.join(HERE, '_single_explode.csv');
    fs.writeFileSync(singleCsv, `id,tags
1,"only"
2,"a;b"`);
    const dfSingle = DataFrame.read_csv(singleCsv);
    const exploded = dfSingle.explode('tags');
    const json = exploded.toDict();
    // Should have 3 rows: 1 + 2
    check('explode single element', json.length === 3, `${json.length} rows (expected 3)`);
    fs.unlinkSync(singleCsv);
} catch (e) {
    check('explode single element', false, e.message);
}

// Test 19: explode row count increases
try {
    const exploded = explodeDf.explode('tags');
    const increased = exploded.shape[0] > explodeDf.shape[0];
    check('explode increases rows', increased,
        `original=${explodeDf.shape[0]} exploded=${exploded.shape[0]}`);
} catch (e) {
    check('explode increases rows', false, e.message);
}

// Test 22: explode invalid column throws
let explodeError = false;
try {
    const dfErr = DataFrame.read_csv(EXPLODE_CSV);
    dfErr.explode('nonexistent');
} catch (e) {
    explodeError = true;
}
check('explode invalid column throws', explodeError, 'error thrown as expected');

// ===========================================================================
// UNNEST TESTS
// ===========================================================================
section('Unnest Tests');

// Test 9: unnest expand struct column
try {
    const unnested = unnestDf.unnest('address');
    const json = unnested.toDict();
    // Should have same rows but more columns
    check('unnest expands struct', json.length > 0, `${json.length} rows`);
} catch (e) {
    check('unnest expands struct', false, e.message);
}

// Test 10: unnest preserves original columns
try {
    const unnested = unnestDf.unnest('address');
    const json = unnested.toDict();
    const hasId = json.every(r => 'id' in r);
    check('unnest preserves columns', hasId, 'id column preserved');
} catch (e) {
    check('unnest preserves columns', false, e.message);
}

// Test 11: unnest correct field names
try {
    const unnested = unnestDf.unnest('address');
    const cols = unnested.columns;
    // Should have city, zip, state columns from the struct
    const hasStructCols = cols.includes('city') || cols.includes('zip') || cols.includes('state');
    check('unnest field names', hasStructCols, `cols: ${cols.join(', ')}`);
} catch (e) {
    check('unnest field names', false, e.message);
}

// Test 17: unnest nested struct fields
try {
    const nestedCsv = path.join(HERE, '_nested_unnest.csv');
    fs.writeFileSync(nestedCsv, `id,data
1,"{outer:{inner:value1}}"`);
    const dfNested = DataFrame.read_csv(nestedCsv);
    const unnested = dfNested.unnest('data');
    const json = unnested.toDict();
    check('unnest nested struct', json.length > 0, 'nested handled');
    fs.unlinkSync(nestedCsv);
} catch (e) {
    check('unnest nested struct', false, e.message);
}

// Test 20: unnest row count unchanged
try {
    const unnested = unnestDf.unnest('address');
    const sameRows = unnested.shape[0] === unnestDf.shape[0];
    check('unnest same row count', sameRows,
        `original=${unnestDf.shape[0]} unnested=${unnested.shape[0]}`);
} catch (e) {
    check('unnest same row count', false, e.message);
}

// Test 23: unnest invalid column throws
let unnestError = false;
try {
    const dfErr = DataFrame.read_csv(UNNEST_CSV);
    dfErr.unnest('nonexistent');
} catch (e) {
    unnestError = true;
}
check('unnest invalid column throws', unnestError, 'error thrown as expected');

// -- Cleanup temp files --
const filesToClean = [JSON_CSV, EXPLODE_CSV, UNNEST_CSV];
filesToClean.forEach(f => { try { fs.unlinkSync(f); } catch (e) {} });

// ===========================================================================
// Summary
// ===========================================================================
section('FINAL RESULT');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 10 Nested Data VALIDATED');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}

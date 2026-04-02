'use strict';

/**
 * PardoX v0.3.2 - Gap 8: Pivot & Melt
 * Validates pivot_table and melt operations via the JavaScript SDK.
 *
 * Tests:
 *  1. pivot_table - basic pivot with sum aggregation
 *  2. pivot_table - pivot with mean aggregation
 *  3. pivot_table - pivot with count aggregation
 *  4. pivot_table - pivot with min aggregation
 *  5. pivot_table - pivot with max aggregation
 *  6. melt - basic melt with single id var
 *  7. melt - melt with multiple id vars
 *  8. melt - custom var_name and value_name
 *  9. pivot_table - output has expected columns
 * 10. melt - output has expected structure
 * 11. pivot_table - handles multiple index values
 * 12. melt - preserves data types
 * 13. pivot_table - null handling
 * 14. melt - empty value_vars handling
 * 15. pivot_table - large dataset pivot
 * 16. melt - reverse of pivot (conceptual)
 * 17. pivot_table - groupby equivalence check
 * 18. melt - all numeric columns melted
 * 19. pivot_table - string index column
 * 20. melt - order preservation
 * 21. pivot_table - zero values handled
 * 22. melt - single value_var
 * 23. pivot_table - result row count equals unique index values
 */

const { DataFrame } = require('@pardox/pardox');
const fs = require('fs');

// -- Create test data for pivot --
// Sales data: region, category, sales, quantity
const PIVOT_CSV = '_pivot_test.csv';
const pivotData = `region,category,sales,quantity
North,Electronics,1000,10
North,Clothing,500,20
South,Electronics,800,8
South,Clothing,600,25
East,Electronics,1200,12
East,Clothing,400,15
West,Electronics,900,9
West,Clothing,550,22
North,Electronics,1100,11
North,Clothing,450,18
South,Electronics,750,7
South,Clothing,650,28
East,Electronics,1300,13
East,Clothing,350,12
West,Electronics,850,8
West,Clothing,500,20`;
fs.writeFileSync(PIVOT_CSV, pivotData);

// -- Create test data for melt --
// Wide format data
const MELT_CSV = '_melt_test.csv';
const meltData = `id,name,math,science,history
1,Alice,90,85,78
2,Bob,88,92,84
3,Charlie,76,88,90`;
fs.writeFileSync(MELT_CSV, meltData);

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

console.log('\n=== Gap 8: Pivot & Melt (JS SDK) ===\n');

const pivotDf = DataFrame.read_csv(PIVOT_CSV);
const meltDf = DataFrame.read_csv(MELT_CSV);

// ===========================================================================
// PIVOT TABLE TESTS
// ===========================================================================
section('Pivot Table Tests');

// Test 1: pivot_table with sum aggregation
try {
    const pivoted = pivotDf.pivotTable('region', 'category', 'sales', 'sum');
    const json = pivoted.toDict();
    // Check that pivoted data exists and has structure
    check('pivot_table sum - returns non-empty result', json.length > 0, `${json.length} rows`);
} catch (e) {
    check('pivot_table sum', false, e.message);
}

// Test 2: pivot_table with mean aggregation
try {
    const pivoted = pivotDf.pivotTable('region', 'category', 'sales', 'mean');
    const json = pivoted.toDict();
    check('pivot_table mean - returns non-empty result', json.length > 0, `${json.length} rows`);
} catch (e) {
    check('pivot_table mean', false, e.message);
}

// Test 3: pivot_table with count aggregation
try {
    const pivoted = pivotDf.pivotTable('region', 'category', 'sales', 'count');
    const json = pivoted.toDict();
    check('pivot_table count - returns non-empty result', json.length > 0, `${json.length} rows`);
} catch (e) {
    check('pivot_table count', false, e.message);
}

// Test 4: pivot_table with min aggregation
try {
    const pivoted = pivotDf.pivotTable('region', 'category', 'sales', 'min');
    const json = pivoted.toDict();
    check('pivot_table min - returns non-empty result', json.length > 0, `${json.length} rows`);
} catch (e) {
    check('pivot_table min', false, e.message);
}

// Test 5: pivot_table with max aggregation
try {
    const pivoted = pivotDf.pivotTable('region', 'category', 'sales', 'max');
    const json = pivoted.toDict();
    check('pivot_table max - returns non-empty result', json.length > 0, `${json.length} rows`);
} catch (e) {
    check('pivot_table max', false, e.message);
}

// Test 9: pivot_table output has expected columns
try {
    const pivoted = pivotDf.pivotTable('region', 'category', 'sales', 'sum');
    const cols = pivoted.columns;
    // Should have region column plus pivoted category columns
    const hasRegion = cols.includes('region');
    check('pivot_table has expected columns', hasRegion, `cols: ${cols.join(', ')}`);
} catch (e) {
    check('pivot_table columns', false, e.message);
}

// Test 11: pivot_table handles multiple index values
try {
    const pivoted = pivotDf.pivotTable('region', 'category', 'sales', 'sum');
    const json = pivoted.toDict();
    const uniqueRegions = new Set(json.map(r => r.region));
    // Should have 4 regions: North, South, East, West
    check('pivot_table multiple index values', uniqueRegions.size === 4, `${uniqueRegions.size} unique regions`);
} catch (e) {
    check('pivot_table multiple index', false, e.message);
}

// Test 13: pivot_table null handling (should not crash)
try {
    const nullCsv = '_pivot_null.csv';
    fs.writeFileSync(nullCsv, 'idx,col,val\nA,X,10\nA,Y,\nB,X,20\nB,Y,30');
    const dfNull = DataFrame.read_csv(nullCsv);
    const pivoted = dfNull.pivotTable('idx', 'col', 'val', 'sum');
    const json = pivoted.toDict();
    check('pivot_table handles nulls', json.length > 0, `${json.length} rows`);
    fs.unlinkSync(nullCsv);
} catch (e) {
    check('pivot_table null handling', false, e.message);
}

// Test 15: pivot_table large dataset
try {
    // Create larger dataset
    let largeCsv = 'region,category,sales\n';
    for (let i = 0; i < 1000; i++) {
        const region = ['North', 'South', 'East', 'West'][i % 4];
        const category = ['A', 'B', 'C'][i % 3];
        const sales = (i * 10) % 1000;
        largeCsv += `${region},${category},${sales}\n`;
    }
    const largePath = '_pivot_large.csv';
    fs.writeFileSync(largePath, largeCsv);
    const dfLarge = DataFrame.read_csv(largePath);
    const pivoted = dfLarge.pivotTable('region', 'category', 'sales', 'sum');
    const json = pivoted.toDict();
    check('pivot_table large dataset', json.length > 0, `${json.length} rows`);
    fs.unlinkSync(largePath);
} catch (e) {
    check('pivot_table large dataset', false, e.message);
}

// Test 17: pivot_table groupby equivalence
try {
    // Sum via pivot should be similar to sum via groupby
    const pivoted = pivotDf.pivotTable('region', 'category', 'sales', 'sum');
    const grouped = pivotDf.groupBy('region', { sales: 'sum' });
    const pivotRows = pivoted.shape[0];
    const groupRows = grouped.shape[0];
    check('pivot_table vs groupby row count', pivotRows === groupRows, `pivot=${pivotRows} groupby=${groupRows}`);
} catch (e) {
    check('pivot_table groupby equivalence', false, e.message);
}

// Test 19: pivot_table string index column
try {
    const pivoted = pivotDf.pivotTable('region', 'category', 'quantity', 'sum');
    const json = pivoted.toDict();
    const hasStringIndex = json.every(r => typeof r.region === 'string');
    check('pivot_table string index', hasStringIndex, 'all region values are strings');
} catch (e) {
    check('pivot_table string index', false, e.message);
}

// Test 21: pivot_table zero values handled
try {
    const zeroCsv = '_pivot_zero.csv';
    fs.writeFileSync(zeroCsv, 'idx,col,val\nA,X,0\nA,Y,10\nB,X,20\nB,Y,0');
    const dfZero = DataFrame.read_csv(zeroCsv);
    const pivoted = dfZero.pivotTable('idx', 'col', 'val', 'sum');
    const json = pivoted.toDict();
    // Check zeros are preserved
    const hasZeros = json.some(r => Object.values(r).some(v => v === 0));
    check('pivot_table zero values', hasZeros, 'zeros preserved in output');
    fs.unlinkSync(zeroCsv);
} catch (e) {
    check('pivot_table zero values', false, e.message);
}

// Test 23: pivot_table result row count equals unique index values
try {
    const pivoted = pivotDf.pivotTable('region', 'category', 'sales', 'sum');
    const json = pivoted.toDict();
    const uniqueIndex = new Set(pivotDf.toDict().map(r => r.region)).size;
    check('pivot_table row count = unique index', json.length === uniqueIndex, `expected ${uniqueIndex}, got ${json.length}`);
} catch (e) {
    check('pivot_table row count', false, e.message);
}

// ===========================================================================
// MELT TESTS
// ===========================================================================
section('Melt Tests');

// Test 6: melt with single id var
try {
    const melted = meltDf.melt(['id'], ['math', 'science', 'history']);
    const json = melted.toDict();
    // 3 rows * 3 value vars = 9 rows
    check('melt single id var', json.length === 9, `${json.length} rows (expected 9)`);
} catch (e) {
    check('melt single id var', false, e.message);
}

// Test 7: melt with multiple id vars
try {
    const melted = meltDf.melt(['id', 'name'], ['math', 'science', 'history']);
    const json = melted.toDict();
    // 3 rows * 3 value vars = 9 rows
    check('melt multiple id vars', json.length === 9, `${json.length} rows (expected 9)`);
} catch (e) {
    check('melt multiple id vars', false, e.message);
}

// Test 8: melt with custom var_name and value_name
try {
    const melted = meltDf.melt(['id'], ['math', 'science'], 'subject', 'score');
    const json = melted.toDict();
    const hasSubject = json[0].hasOwnProperty('subject');
    const hasScore = json[0].hasOwnProperty('score');
    check('melt custom names', hasSubject && hasScore, 'subject and score columns exist');
} catch (e) {
    check('melt custom names', false, e.message);
}

// Test 10: melt output has expected structure
try {
    const melted = meltDf.melt(['id'], ['math', 'science', 'history']);
    const json = melted.toDict();
    const cols = melted.columns;
    // Should have id, variable, value columns
    const hasExpectedCols = cols.includes('id') && cols.includes('variable') && cols.includes('value');
    check('melt expected structure', hasExpectedCols, `cols: ${cols.join(', ')}`);
} catch (e) {
    check('melt structure', false, e.message);
}

// Test 12: melt preserves data types
try {
    const melted = meltDf.melt(['id', 'name'], ['math', 'science', 'history']);
    const json = melted.toDict();
    // Check numeric values are numbers
    const allNumeric = json.every(r => typeof r.value === 'number');
    check('melt preserves types', allNumeric, 'all value entries are numeric');
} catch (e) {
    check('melt preserves types', false, e.message);
}

// Test 14: melt empty value_vars handling
let emptyValueVarsError = false;
try {
    const melted = meltDf.melt(['id'], []);
} catch (e) {
    emptyValueVarsError = true;
}
check('melt empty value_vars throws', emptyValueVarsError, 'error thrown as expected');

// Test 16: melt reverse of pivot (conceptual)
try {
    // Melt should produce more rows than original wide format
    const melted = meltDf.melt(['id', 'name'], ['math', 'science', 'history']);
    const originalRows = meltDf.shape[0];
    const meltedRows = melted.shape[0];
    check('melt increases rows', meltedRows > originalRows, `original=${originalRows} melted=${meltedRows}`);
} catch (e) {
    check('melt reverse of pivot', false, e.message);
}

// Test 18: melt all numeric columns melted
try {
    const melted = meltDf.melt(['id'], ['math', 'science', 'history']);
    const json = melted.toDict();
    const allHaveValue = json.every(r => 'value' in r);
    check('melt all columns melted', allHaveValue, 'all rows have value column');
} catch (e) {
    check('melt all columns', false, e.message);
}

// Test 20: melt order preservation
try {
    const melted = meltDf.melt(['id'], ['math', 'science', 'history']);
    const json = melted.toDict();
    // First 3 rows should be id=1, id=2, id=3 in order
    const firstThree = json.slice(0, 3).map(r => r.id);
    const ordered = firstThree[0] === 1 && firstThree[1] === 2 && firstThree[2] === 3;
    check('melt order preservation', ordered, `first three ids: ${firstThree.join(', ')}`);
} catch (e) {
    check('melt order preservation', false, e.message);
}

// Test 22: melt single value_var
try {
    const melted = meltDf.melt(['id'], ['math']);
    const json = melted.toDict();
    // 3 rows * 1 value var = 3 rows
    check('melt single value_var', json.length === 3, `${json.length} rows (expected 3)`);
} catch (e) {
    check('melt single value_var', false, e.message);
}

// -- Cleanup temp files --
try { fs.unlinkSync(PIVOT_CSV); } catch (e) {}
try { fs.unlinkSync(MELT_CSV); } catch (e) {}

// ===========================================================================
// Summary
// ===========================================================================
section('FINAL RESULT');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 8 Pivot & Melt VALIDATED');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}

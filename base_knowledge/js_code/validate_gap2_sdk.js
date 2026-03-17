'use strict';

/**
 * PardoX v0.3.2 - Gap 2: String & Date Functions Validation
 * Validates string and date functions using the JS SDK.
 *
 * String Functions:
 *  - str_upper, str_lower, str_contains, str_replace, str_trim, str_len
 *
 * Date Functions:
 *  - date_extract (year/month/day/weekday), date_format, date_add, date_diff
 */

const { DataFrame, getLib } = require('@pardox/pardox');
const fs = require('fs');

const CUSTOMERS_CSV = 'customers.csv';
const SALES_CSV = 'sales.csv';

let passed = 0;
let failed = 0;

function section(title) {
    console.log(`\n${'='.repeat(66)}`);
    console.log(`  ${title}`);
    console.log('='.repeat(66));
}

function ok(name, detail = '') {
    passed++;
    console.log(`  [OK]   ${name}${detail ? '  - ' + detail : ''}`);
}

function fail(name, detail = '') {
    failed++;
    console.log(`  [FAIL] ${name}${detail ? '  - ' + detail : ''}`);
}

function check(result, name, expectedRows = null) {
    if (!result) {
        fail(name, 'returned null');
        return false;
    }
    const rows = result.shape ? result.shape[0] : 0;
    if (expectedRows !== null && rows !== expectedRows) {
        fail(name, `expected ${expectedRows} rows, got ${rows}`);
        return false;
    }
    ok(name, `${rows.toLocaleString()} rows`);
    return true;
}

function getPtr(df) {
    const { PTR_SYMBOL } = require('@pardox/pardox');
    return df[PTR_SYMBOL];
}

console.log('\n[INIT] Starting PardoX engine (JS SDK)...');

section('SETUP: Load CSVs');
const customers = DataFrame.read_csv(CUSTOMERS_CSV);
const sales = DataFrame.read_csv(SALES_CSV);
console.log(`  customers : ${customers.shape[0].toLocaleString()} rows`);
console.log(`  sales     : ${sales.shape[0].toLocaleString()} rows`);

const lib = getLib();

// =============================================================================
// PART 1: STRING FUNCTIONS
// =============================================================================

section('STR 1 - str_upper("first_name")');
try {
    const ptr = lib.pardox_str_upper(getPtr(customers), 'first_name');
    const result = new DataFrame(ptr);
    check(result, 'str_upper');
    result._free();
} catch (e) {
    fail('str_upper', e.message);
}

section('STR 2 - str_lower("state")');
try {
    const ptr = lib.pardox_str_lower(getPtr(customers), 'state');
    const result = new DataFrame(ptr);
    check(result, 'str_lower');
    result._free();
} catch (e) {
    fail('str_lower', e.message);
}

section('STR 3 - str_contains("email", "@example")');
try {
    const ptr = lib.pardox_str_contains(getPtr(customers), 'email', '@example');
    const result = new DataFrame(ptr);
    check(result, 'str_contains @example');
    result._free();
} catch (e) {
    fail('str_contains', e.message);
}

section('STR 4 - str_contains("category", "Electronics")');
try {
    const ptr = lib.pardox_str_contains(getPtr(sales), 'category', 'Electronics');
    const result = new DataFrame(ptr);
    check(result, 'str_contains Electronics');
    result._free();
} catch (e) {
    fail('str_contains Electronics', e.message);
}

section('STR 5 - str_replace("email", "@example.com", "@pardox.io")');
try {
    const ptr = lib.pardox_str_replace(getPtr(customers), 'email', '@example.com', '@pardox.io');
    const result = new DataFrame(ptr);
    check(result, 'str_replace');
    result._free();
} catch (e) {
    fail('str_replace', e.message);
}

section('STR 6 - str_trim("city")');
try {
    const ptr = lib.pardox_str_trim(getPtr(customers), 'city');
    const result = new DataFrame(ptr);
    check(result, 'str_trim');
    result._free();
} catch (e) {
    fail('str_trim', e.message);
}

section('STR 7 - str_len("first_name")');
try {
    const ptr = lib.pardox_str_len(getPtr(customers), 'first_name');
    const result = new DataFrame(ptr);
    check(result, 'str_len first_name');
    result._free();
} catch (e) {
    fail('str_len first_name', e.message);
}

section('STR 8 - str_len("category")');
try {
    const ptr = lib.pardox_str_len(getPtr(sales), 'category');
    const result = new DataFrame(ptr);
    check(result, 'str_len category');
    result._free();
} catch (e) {
    fail('str_len category', e.message);
}

// =============================================================================
// PART 2: DATE FUNCTIONS
// =============================================================================

section('SETUP: Build date test data (5 rows, two date columns)');

const sampleDatesA = ['2025-07-22', '2022-04-27', '2023-10-15', '2024-01-01', '2021-06-30'];
const sampleDatesB = ['2025-01-01', '2022-01-01', '2023-01-01', '2024-01-01', '2021-01-01'];

function epochDays(d) {
    const date = new Date(d);
    const epoch = new Date('1970-01-01');
    return Math.floor((date - epoch) / (1000 * 60 * 60 * 24));
}

const names = ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'];
const cities = ['  New York  ', '  London  ', '  Berlin  ', '  Tokyo  ', '  Paris  '];

// Write temp CSV
const dateCsvPath = '_date_test.csv';

const csvContent = 'name,date_a,date_b,city_padded\n' +
    names.map((name, i) =>
        `${name},${epochDays(sampleDatesA[i])},${epochDays(sampleDatesB[i])},${cities[i]}`
    ).join('\n');

fs.writeFileSync(dateCsvPath, csvContent);

// Schema format required by Rust: { column_names: [...], column_types: [...] }
const dateSchemaJson = JSON.stringify({
    column_names: ['name', 'date_a', 'date_b', 'city_padded'],
    column_types: ['Utf8', 'Int64', 'Int64', 'Utf8']
});

const DEFAULT_CSV_CONFIG = JSON.stringify({
    delimiter: 44,
    quote_char: 34,
    has_header: true,
    chunk_size: 16 * 1024 * 1024,
});

const datesPtr = lib.pardox_load_manager_csv(dateCsvPath, dateSchemaJson, DEFAULT_CSV_CONFIG);
const datesDf = new DataFrame(datesPtr);
console.log(`  Loaded date test data: ${datesDf.shape[0]} rows`);

// DATE 1: date_extract year
section('DATE 1 - date_extract("date_a", "year")');
try {
    const ptr = lib.pardox_date_extract(getPtr(datesDf), 'date_a', 'year');
    const result = new DataFrame(ptr);
    check(result, 'date_extract year');
    result._free();
} catch (e) {
    fail('date_extract year', e.message);
}

// DATE 2: date_extract month
section('DATE 2 - date_extract("date_a", "month")');
try {
    const ptr = lib.pardox_date_extract(getPtr(datesDf), 'date_a', 'month');
    const result = new DataFrame(ptr);
    check(result, 'date_extract month');
    result._free();
} catch (e) {
    fail('date_extract month', e.message);
}

// DATE 3: date_extract day
section('DATE 3 - date_extract("date_a", "day")');
try {
    const ptr = lib.pardox_date_extract(getPtr(datesDf), 'date_a', 'day');
    const result = new DataFrame(ptr);
    check(result, 'date_extract day');
    result._free();
} catch (e) {
    fail('date_extract day', e.message);
}

// DATE 4: date_extract weekday
section('DATE 4 - date_extract("date_a", "weekday")');
try {
    const ptr = lib.pardox_date_extract(getPtr(datesDf), 'date_a', 'weekday');
    const result = new DataFrame(ptr);
    check(result, 'date_extract weekday');
    result._free();
} catch (e) {
    fail('date_extract weekday', e.message);
}

// DATE 5: date_format
section('DATE 5 - date_format("date_a", "%Y-%m-%d")');
try {
    const ptr = lib.pardox_date_format(getPtr(datesDf), 'date_a', '%Y-%m-%d');
    const result = new DataFrame(ptr);
    check(result, 'date_format');
    result._free();
} catch (e) {
    fail('date_format', e.message);
}

// DATE 6: date_format month-year
section('DATE 6 - date_format("date_a", "%B %Y")');
try {
    const ptr = lib.pardox_date_format(getPtr(datesDf), 'date_a', '%B %Y');
    const result = new DataFrame(ptr);
    check(result, 'date_format %B %Y');
    result._free();
} catch (e) {
    fail('date_format %B %Y', e.message);
}

// DATE 7: date_add +30 days
section('DATE 7 - date_add("date_a", +30)');
try {
    const ptr = lib.pardox_date_add(getPtr(datesDf), 'date_a', 30);
    const result = new DataFrame(ptr);
    check(result, 'date_add +30 days');
    result._free();
} catch (e) {
    fail('date_add +30 days', e.message);
}

// DATE 8: date_add -365 days
section('DATE 8 - date_add("date_a", -365)');
try {
    const ptr = lib.pardox_date_add(getPtr(datesDf), 'date_a', -365);
    const result = new DataFrame(ptr);
    check(result, 'date_add -365 days');
    result._free();
} catch (e) {
    fail('date_add -365 days', e.message);
}

// DATE 9: date_diff days
section('DATE 9 - date_diff("date_a", "date_b", "days")');
try {
    const ptr = lib.pardox_date_diff(getPtr(datesDf), 'date_a', 'date_b', 'days');
    const result = new DataFrame(ptr);
    check(result, 'date_diff days');
    result._free();
} catch (e) {
    fail('date_diff days', e.message);
}

// DATE 10: date_diff months
section('DATE 10 - date_diff("date_a", "date_b", "months")');
try {
    const ptr = lib.pardox_date_diff(getPtr(datesDf), 'date_a', 'date_b', 'months');
    const result = new DataFrame(ptr);
    check(result, 'date_diff months');
    result._free();
} catch (e) {
    fail('date_diff months', e.message);
}

// Cleanup
customers._free();
sales._free();
datesDf._free();
fs.unlinkSync(dateCsvPath);

section('FINAL RESULT - Gap 2');
console.log(`  Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
    console.log('  ALL TESTS PASSED - Gap 2 VALIDATED');
    process.exit(0);
} else {
    console.log(`  ${failed} TEST(S) FAILED`);
    process.exit(1);
}

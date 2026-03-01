'use strict';

/**
 * PardoX — Universal High-Performance DataFrame Engine for Node.js
 *
 * Usage:
 *   const { DataFrame, Series, read_csv, read_sql, read_prdx } = require('@pardox/pardox');
 *
 *   // Load from CSV
 *   const df = read_csv('./data.csv');
 *   df.show();
 *
 *   // Column access (Proxy-based subscript)
 *   const col = df['price'];    // → Series
 *   df['tax'] = df['price'].mul(df['rate']);  // column assignment
 *
 *   // Aggregations
 *   console.log(df['price'].sum());
 *   console.log(df['price'].mean());
 *
 *   // Filtering
 *   const mask = df['price'].gt(100);
 *   const filtered = df.filter(mask);
 *
 *   // Write to PostgreSQL
 *   df.toSql('postgresql://user:pass@localhost:5432/db', 'products', 'upsert', ['id']);
 */

const { DataFrame } = require('./DataFrame');
const { Series }    = require('./Series');
const {
    read_csv,
    read_sql,
    read_prdx,
    executeSql,
    read_mysql,
    execute_mysql,
    read_sqlserver,
    execute_sqlserver,
    read_mongodb,
    execute_mongodb,
} = require('./io');
const { getLib } = require('./ffi');

module.exports = {
    // Core classes
    DataFrame,
    Series,

    // IO helpers (functional API, mirrors Python's pardox.read_csv())
    read_csv,
    read_sql,
    read_prdx,
    executeSql,
    read_mysql,
    execute_mysql,
    read_sqlserver,
    execute_sqlserver,
    read_mongodb,
    execute_mongodb,

    // Low-level access (for advanced use)
    getLib,
};

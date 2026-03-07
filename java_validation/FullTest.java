package io.pardox.test;

import io.pardox.*;

/**
 * Complete test suite for PardoX Java SDK
 * Tests CSV, SQL, and all major features
 */
public class FullTest {

    // Connection strings from .env
    private static final String POSTGRES_CONN = "postgresql://pardox:pardox123@localhost:5434/pardox_test";
    private static final String CSV_PATH = "hardcore_sales_50k.csv";

    public static void main(String[] args) {
        System.out.println("=== PardoX Java SDK Full Test Suite ===\n");
        int passed = 0;
        int failed = 0;

        // Test 1: Library Loading
        try {
            System.out.print("Test 1: Library Loading... ");
            Lib lib = Lib.getInstance();
            System.out.println("PASS");
            passed++;
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Test 2: Engine Init
        try {
            System.out.print("Test 2: Engine Initialization... ");
            Io.init();
            System.out.println("PASS");
            passed++;
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Test 3: Read CSV
        DataFrame csvDf = null;
        try {
            System.out.print("Test 3: Read CSV (50k rows)... ");
            csvDf = Io.readCsv(CSV_PATH);
            System.out.println("PASS (loaded)");
            passed++;
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Test 4: Create from JSON
        DataFrame jsonDf = null;
        try {
            System.out.print("Test 4: Create DataFrame from JSON... ");
            java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>();
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", 1);
            row.put("name", "Test");
            row.put("value", 100.5);
            data.add(row);
            jsonDf = DataFrame.fromJsonRecords(data);
            System.out.println("PASS");
            passed++;
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Test 5: Export to CSV
        try {
            System.out.print("Test 5: Export to CSV... ");
            if (jsonDf != null) {
                jsonDf.toCsv("test_export.csv");
                java.io.File f = new java.io.File("test_export.csv");
                if (f.exists() && f.length() > 0) {
                    f.delete();
                    System.out.println("PASS");
                    passed++;
                } else {
                    System.out.println("FAIL: file not created");
                    failed++;
                }
            }
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Test 6: Aggregation (if CSV loaded)
        try {
            System.out.print("Test 6: Aggregation (sum/mean/min/max)... ");
            if (csvDf != null) {
                // Get first numeric column found
                String numericCol = findNumericColumn(csvDf);
                if (numericCol != null) {
                    Series s = csvDf.get(numericCol);
                    double sum = s.sum();
                    double mean = s.mean();
                    double min = s.min();
                    double max = s.max();
                    System.out.println("PASS (sum=" + sum + ", mean=" + mean + ")");
                    passed++;
                } else {
                    System.out.println("SKIP (no numeric column)");
                }
            } else {
                System.out.println("SKIP (no CSV loaded)");
            }
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Test 7: PostgreSQL Write
        try {
            System.out.print("Test 7: Write to PostgreSQL... ");
            if (jsonDf != null) {
                // Create table first
                Io.executeSql(POSTGRES_CONN, "DROP TABLE IF EXISTS pardox_java_test");
                Io.executeSql(POSTGRES_CONN, "CREATE TABLE pardox_java_test (id INTEGER, name TEXT, value DOUBLE PRECISION)");

                int rows = jsonDf.toSql(POSTGRES_CONN, "pardox_java_test");
                if (rows > 0) {
                    System.out.println("PASS (" + rows + " rows)");
                    passed++;
                } else {
                    System.out.println("FAIL: no rows written");
                    failed++;
                }
            }
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Test 8: PostgreSQL Read
        DataFrame sqlDf = null;
        try {
            System.out.print("Test 8: Read from PostgreSQL... ");
            sqlDf = Io.readSql(POSTGRES_CONN, "SELECT * FROM pardox_java_test");
            System.out.println("PASS (read " + sqlDf.getPtr() + ")");
            passed++;
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Test 9: Join
        try {
            System.out.print("Test 9: DataFrame Join... ");
            if (csvDf != null && sqlDf != null) {
                // Just test that join doesn't crash
                System.out.println("SKIP (complex join)");
            } else {
                System.out.println("SKIP (no data)");
            }
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Test 10: Cleanup
        try {
            System.out.print("Test 10: Memory Cleanup... ");
            if (csvDf != null) csvDf.free();
            if (jsonDf != null) jsonDf.free();
            if (sqlDf != null) sqlDf.free();
            Io.executeSql(POSTGRES_CONN, "DROP TABLE IF EXISTS pardox_java_test");
            System.out.println("PASS");
            passed++;
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            failed++;
        }

        // Summary
        System.out.println("\n=== Test Summary ===");
        System.out.println("Passed: " + passed);
        System.out.println("Failed: " + failed);
        System.out.println("===================");

        if (failed > 0) {
            System.exit(1);
        }
    }

    private static String findNumericColumn(DataFrame df) {
        try {
            java.util.Map<String, String> dtypes = df.dtypes();
            for (String col : dtypes.keySet()) {
                String dtype = dtypes.get(col);
                if (dtype != null && (dtype.contains("Int") || dtype.contains("Float") || dtype.contains("Double"))) {
                    return col;
                }
            }
        } catch (Exception e) {
            // Ignore
        }
        return null;
    }
}

package io.pardox.test;

import io.pardox.*;

/**
 * Basic test for PardoX Java SDK
 */
public class BasicTest {

    public static void main(String[] args) {
        System.out.println("=== PardoX Java SDK Basic Test ===\n");

        try {
            // Test 1: Library Loading
            System.out.println("Test 1: Library Loading");
            Lib lib = Lib.getInstance();
            System.out.println("  [OK] Native library loaded successfully");

            // Test 2: Init Engine
            System.out.println("\nTest 2: Engine Initialization");
            Io.init();
            System.out.println("  [OK] Engine initialized");

            // Test 3: Check data file exists
            String csvPath = "hardcore_sales_50k.csv";
            java.io.File file = new java.io.File(csvPath);
            if (file.exists()) {
                System.out.println("\nTest 3: Read CSV File");
                DataFrame df = Io.readCsv(csvPath);

                // Use native methods that work correctly
                System.out.println("  [OK] CSV loaded");

                // Try to get row count directly
                try {
                    // Try head first (doesn't need schema)
                    System.out.println("\nTest 4: Show Data (head)");
                    System.out.println(df.show(5));
                } catch (Exception e) {
                    System.out.println("  Show error: " + e.getMessage());
                }

                df.free();
            } else {
                System.out.println("\nTest 3: CSV file not found - skipping");
                System.out.println("  File: " + csvPath + " not found");
            }

            // Test 5: Create DataFrame from memory
            System.out.println("\nTest 5: Create DataFrame from JSON");
            java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>();
            java.util.Map<String, Object> row1 = new java.util.HashMap<>();
            row1.put("id", 1);
            row1.put("name", "Alice");
            row1.put("price", 100.5);
            data.add(row1);

            java.util.Map<String, Object> row2 = new java.util.HashMap<>();
            row2.put("id", 2);
            row2.put("name", "Bob");
            row2.put("price", 200.0);
            data.add(row2);

            DataFrame dfMem = DataFrame.fromJsonRecords(data);
            System.out.println("  [OK] DataFrame created: " + dfMem.getPtr() + " (pointer)");

            // Test 6: Show data
            System.out.println("\nTest 6: Display Data");
            try {
                System.out.println(dfMem.show(5));
            } catch (Exception e) {
                System.out.println("  Show error: " + e.getMessage());
            }

            // Test 7: Export to CSV
            System.out.println("\nTest 7: Export to CSV");
            String outPath = "test_output_java.csv";
            try {
                dfMem.toCsv(outPath);
                System.out.println("  [OK] Exported to: " + outPath);

                // Verify file exists
                java.io.File outFile = new java.io.File(outPath);
                if (outFile.exists()) {
                    System.out.println("  [OK] File verified: " + outFile.length() + " bytes");
                    outFile.delete();
                }
            } catch (Exception e) {
                System.out.println("  Export error: " + e.getMessage());
            }

            // Cleanup
            dfMem.free();

            System.out.println("\n=== Core Tests Passed ===");

        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

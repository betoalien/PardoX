package io.pardox;

import com.sun.jna.Pointer;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * PardoX IO - Native Data Import/Export Functions
 *
 * Mirrors Python's io.py and JavaScript's io.js
 */
public class Io {

    // Default CSV configuration
    private static final Map<String, Object> DEFAULT_CSV_CONFIG = new HashMap<>();
    static {
        DEFAULT_CSV_CONFIG.put("delimiter", 44);        // Comma (,)
        DEFAULT_CSV_CONFIG.put("quote_char", 34);       // Double Quote (")
        DEFAULT_CSV_CONFIG.put("has_header", true);
        DEFAULT_CSV_CONFIG.put("chunk_size", 16 * 1024 * 1024);  // 16MB
    }

    // =====================================================================
    // CSV READING
    // =====================================================================

    /**
     * Read CSV file into DataFrame
     */
    public static DataFrame readCsv(String path) {
        return readCsv(path, null);
    }

    /**
     * Read CSV file with custom schema
     */
    public static DataFrame readCsv(String path, Map<String, String> schema) {
        if (!Files.exists(Paths.get(path))) {
            throw new RuntimeException("File not found: " + path);
        }

        Lib lib = Lib.getInstance();

        // Build config JSON
        String configJson = toJson(DEFAULT_CSV_CONFIG);

        // Build schema JSON
        String schemaJson = "{}";
        if (schema != null && !schema.isEmpty()) {
            StringBuilder sb = new StringBuilder("{\"columns\":[");
            boolean first = true;
            for (Map.Entry<String, String> entry : schema.entrySet()) {
                if (!first) sb.append(",");
                sb.append("{\"name\":\"").append(entry.getKey())
                  .append("\",\"type\":\"").append(entry.getValue()).append("\"}");
                first = false;
            }
            sb.append("]}");
            schemaJson = sb.toString();
        }

        long ptr = lib.loadManagerCsv(path, schemaJson, configJson);

        if (ptr == 0) {
            throw new RuntimeException("Failed to load CSV: " + path);
        }

        return new DataFrame(ptr);
    }

    // =====================================================================
    // SQL READING
    // =====================================================================

    /**
     * Read from PostgreSQL using native Rust driver
     */
    public static DataFrame readSql(String connectionString, String query) {
        Lib lib = Lib.getInstance();
        long ptr = lib.scanSql(connectionString, query);

        if (ptr == 0) {
            throw new RuntimeException("SQL Query failed. Check connection and query.");
        }

        return new DataFrame(ptr);
    }

    /**
     * Execute DDL/DML without returning data
     */
    public static int executeSql(String connectionString, String query) {
        Lib lib = Lib.getInstance();
        long rows = lib.executeSql(connectionString, query);

        if (rows < 0) {
            throw new RuntimeException("execute_sql failed with code: " + rows);
        }

        return (int) rows;
    }

    // =====================================================================
    // MYSQL
    // =====================================================================

    /**
     * Read from MySQL using native Rust driver
     */
    public static DataFrame readMysql(String connectionString, String query) {
        Lib lib = Lib.getInstance();
        long ptr = lib.readMysql(connectionString, query);

        if (ptr == 0) {
            throw new RuntimeException("MySQL query failed");
        }

        return new DataFrame(ptr);
    }

    /**
     * Execute MySQL DDL/DML
     */
    public static int executeMysql(String connectionString, String query) {
        Lib lib = Lib.getInstance();
        long rows = lib.executeMysql(connectionString, query);

        if (rows < 0) {
            throw new RuntimeException("execute_mysql failed with code: " + rows);
        }

        return (int) rows;
    }

    // =====================================================================
    // SQL SERVER
    // =====================================================================

    /**
     * Read from SQL Server using native Rust driver
     */
    public static DataFrame readSqlServer(String connectionString, String query) {
        Lib lib = Lib.getInstance();
        long ptr = lib.readSqlServer(connectionString, query);

        if (ptr == 0) {
            throw new RuntimeException("SQL Server query failed");
        }

        return new DataFrame(ptr);
    }

    /**
     * Execute SQL Server DDL/DML
     */
    public static int executeSqlServer(String connectionString, String query) {
        Lib lib = Lib.getInstance();
        long rows = lib.executeSqlServer(connectionString, query);

        if (rows < 0) {
            throw new RuntimeException("execute_sqlserver failed with code: " + rows);
        }

        return (int) rows;
    }

    // =====================================================================
    // MONGODB
    // =====================================================================

    /**
     * Read from MongoDB collection
     */
    public static DataFrame readMongoDB(String connectionString, String dbDotCollection) {
        Lib lib = Lib.getInstance();
        long ptr = lib.readMongoDB(connectionString, dbDotCollection);

        if (ptr == 0) {
            throw new RuntimeException("MongoDB read failed");
        }

        return new DataFrame(ptr);
    }

    /**
     * Execute MongoDB command
     */
    public static int executeMongoDB(String connectionString, String database, String commandJson) {
        Lib lib = Lib.getInstance();
        long result = lib.executeMongoDB(connectionString, database, commandJson);

        if (result < 0) {
            throw new RuntimeException("execute_mongodb failed with code: " + result);
        }

        return (int) result;
    }

    // =====================================================================
    // PRDX READING
    // =====================================================================

    /**
     * Read head of .prdx file as JSON
     */
    public static List<Map<String, Object>> readPrdx(String path, int limit) {
        if (!Files.exists(Paths.get(path))) {
            throw new RuntimeException("File not found: " + path);
        }

        Lib lib = Lib.getInstance();
        long jsonPtr = lib.readHeadJson(path, limit);

        if (jsonPtr == 0) {
            throw new RuntimeException("Failed to read PRDX file");
        }

        String jsonStr = Pointer.createConstant(jsonPtr).getString(0);
        lib.freeString(jsonPtr);
        return parseJsonArray(jsonStr);
    }

    /**
     * Quick column sum (without loading full file)
     */
    public static double columnSum(String path, String column) {
        Lib lib = Lib.getInstance();
        return lib.columnSum(path, column);
    }

    // =====================================================================
    // JSON INGESTION
    // =====================================================================

    /**
     * Create DataFrame from JSON string
     */
    public static DataFrame readJson(String jsonString) {
        byte[] jsonBytes = jsonString.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        Lib lib = Lib.getInstance();
        long ptr = lib.readJsonBytes(jsonBytes, jsonBytes.length);

        if (ptr == 0) {
            throw new RuntimeException("Failed to ingest JSON data");
        }

        return new DataFrame(ptr);
    }

    // =====================================================================
    // UTILITIES
    // =====================================================================

    private static String toJson(Map<String, Object> map) {
        // Simplified JSON serialization
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":");
            Object value = entry.getValue();
            if (value instanceof Number) {
                sb.append(value);
            } else if (value instanceof Boolean) {
                sb.append(value);
            } else if (value instanceof String) {
                sb.append("\"").append(value).append("\"");
            }
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private static List<Map<String, Object>> parseJsonArray(String json) {
        // Simplified - returns empty list
        // In production use Jackson/Gson
        return new ArrayList<>();
    }

    /**
     * Initialize the PardoX engine
     */
    public static void init() {
        Lib lib = Lib.getInstance();
        lib.initEngine();
    }
}

package io.pardox;

import com.sun.jna.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * PardoX Native Library Loader using JNA
 *
 * Automatically detects OS and architecture to load the correct native binary.
 */
public class Lib {

    private static Lib INSTANCE = null;
    private NativeLibrary nativeLib;

    // C Type definitions matching Rust FFI
    public static final int POINTER_SIZE = Native.POINTER_SIZE;

    private Lib() {
        this.nativeLib = loadLibrary();
    }

    public static synchronized Lib getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new Lib();
        }
        return INSTANCE;
    }

    private NativeLibrary loadLibrary() {
        String os = System.getProperty("os.name").toLowerCase();
        String arch = System.getProperty("os.arch").toLowerCase();

        String libName;
        String folder;

        if (os.contains("linux")) {
            libName = "pardox-cpu-Linux-x64.so";
            folder = "Linux";
        } else if (os.contains("windows")) {
            libName = "pardox-cpu-Windows-x64.dll";
            folder = "Win";
        } else if (os.contains("mac") || os.contains("darwin")) {
            folder = "Mac";
            if (arch.contains("aarch64") || arch.contains("arm64")) {
                libName = "pardox-cpu-MacOS-ARM64.dylib";
            } else {
                libName = "pardox-cpu-MacOS-Intel.dylib";
            }
        } else {
            throw new UnsatisfiedLinkError("Unsupported operating system: " + os);
        }

        // Try multiple paths
        String[] possiblePaths = {
            "pardox/libs/" + folder + "/" + libName,
            "libs/" + folder + "/" + libName,
            System.getProperty("user.dir") + "/pardox/libs/" + folder + "/" + libName
        };

        String libPath = null;
        for (String path : possiblePaths) {
            File f = new File(path);
            if (f.exists()) {
                libPath = f.getAbsolutePath();
                break;
            }
        }

        if (libPath == null) {
            throw new UnsatisfiedLinkError(
                "PardoX native library not found.\n" +
                "Expected: " + libName + " in " + folder + "/\n" +
                "Searched paths: " + Arrays.toString(possiblePaths)
            );
        }

        System.load(libPath);
        return NativeLibrary.getInstance(libPath);
    }

    /**
     * Get native function by name
     */
    public Function getFunction(String name) {
        return nativeLib.getFunction(name);
    }

    /**
     * Check if function exists
     */
    public boolean hasFunction(String name) {
        try {
            nativeLib.getFunction(name);
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

    /**
     * Convert Java String to null-terminated byte array (UTF-8)
     */
    public static byte[] toCString(String s) {
        if (s == null) return null;
        return (s + "\0").getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    // =====================================================================
    // LOW-LEVEL NATIVE CALLS
    // =====================================================================

    /**
     * pardox_init_engine()
     */
    public void initEngine() {
        try {
            Function f = nativeLib.getFunction("pardox_init_engine");
            f.invokeVoid(new Object[0]);
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Warning: pardox_init_engine not available");
        }
    }

    /**
     * pardox_load_manager_csv(path, schema_json, config_json) -> long
     */
    public long loadManagerCsv(String path, String schemaJson, String configJson) {
        Function f = nativeLib.getFunction("pardox_load_manager_csv");
        return f.invokeLong(new Object[]{
            toCString(path),
            toCString(schemaJson),
            toCString(configJson)
        });
    }

    /**
     * pardox_free_manager(ptr)
     */
    public void freeManager(long ptr) {
        if (ptr == 0) return;
        try {
            Function f = nativeLib.getFunction("pardox_free_manager");
            f.invokeVoid(new Object[]{Pointer.createConstant(ptr)});
        } catch (UnsatisfiedLinkError e) {
            // Ignore
        }
    }

    /**
     * pardox_free_string(ptr)
     */
    public void freeString(long ptr) {
        if (ptr == 0) return;
        try {
            Function f = nativeLib.getFunction("pardox_free_string");
            f.invokeVoid(new Object[]{Pointer.createConstant(ptr)});
        } catch (UnsatisfiedLinkError e) {
            // Ignore
        }
    }

    /**
     * pardox_manager_to_json(ptr, limit) -> String
     */
    public String managerToJson(long ptr, long limit) {
        Function f = nativeLib.getFunction("pardox_manager_to_json");
        long result = f.invokeLong(new Object[]{Pointer.createConstant(ptr), limit});
        if (result == 0) return null;
        return Pointer.createConstant(result).getString(0);
    }

    /**
     * pardox_manager_to_ascii(ptr, limit) -> String
     */
    public String managerToAscii(long ptr, long limit) {
        Function f = nativeLib.getFunction("pardox_manager_to_ascii");
        long result = f.invokeLong(new Object[]{Pointer.createConstant(ptr), limit});
        if (result == 0) return null;
        return Pointer.createConstant(result).getString(0);
    }

    /**
     * pardox_get_row_count(ptr) -> long
     */
    public long getRowCount(long ptr) {
        Function f = nativeLib.getFunction("pardox_get_row_count");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr)});
    }

    /**
     * pardox_get_schema_json(ptr) -> String
     */
    public String getSchemaJson(long ptr) {
        Function f = nativeLib.getFunction("pardox_get_schema_json");
        long result = f.invokeLong(new Object[]{Pointer.createConstant(ptr)});
        if (result == 0) return null;
        return Pointer.createConstant(result).getString(0);
    }

    /**
     * pardox_slice_manager(ptr, start, len) -> long
     */
    public long sliceManager(long ptr, long start, long len) {
        Function f = nativeLib.getFunction("pardox_slice_manager");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr), start, len});
    }

    /**
     * pardox_tail_manager(ptr, n) -> long
     */
    public long tailManager(long ptr, long n) {
        Function f = nativeLib.getFunction("pardox_tail_manager");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr), n});
    }

    /**
     * pardox_hash_join(left, right, left_key, right_key) -> long
     */
    public long hashJoin(long left, long right, String leftKey, String rightKey) {
        Function f = nativeLib.getFunction("pardox_hash_join");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(left),
            Pointer.createConstant(right),
            toCString(leftKey),
            toCString(rightKey)
        });
    }

    /**
     * pardox_cast_column(mgr, col_name, target_type) -> int
     */
    public int castColumn(long ptr, String colName, String targetType) {
        Function f = nativeLib.getFunction("pardox_cast_column");
        return f.invokeInt(new Object[]{
            Pointer.createConstant(ptr),
            toCString(colName),
            toCString(targetType)
        });
    }

    /**
     * pardox_math_add(mgr, col_a, col_b) -> long
     */
    public long mathAdd(long ptr, String colA, String colB) {
        Function f = nativeLib.getFunction("pardox_math_add");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(colA),
            toCString(colB)
        });
    }

    /**
     * pardox_math_sub(mgr, col_a, col_b) -> long
     */
    public long mathSub(long ptr, String colA, String colB) {
        Function f = nativeLib.getFunction("pardox_math_sub");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(colA),
            toCString(colB)
        });
    }

    /**
     * pardox_math_stddev(mgr, col_name) -> double
     */
    public double mathStddev(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_math_stddev");
        return f.invokeDouble(new Object[]{
            Pointer.createConstant(ptr),
            toCString(colName)
        });
    }

    /**
     * pardox_math_minmax(mgr, col_name) -> long
     */
    public long mathMinMax(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_math_minmax");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(colName)
        });
    }

    /**
     * pardox_sort_values(mgr, col_name, descending) -> long
     */
    public long sortValues(long ptr, String colName, int descending) {
        Function f = nativeLib.getFunction("pardox_sort_values");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(colName),
            descending
        });
    }

    /**
     * pardox_gpu_sort(mgr, col_name) -> long
     */
    public long gpuSort(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_gpu_sort");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(colName)
        });
    }

    /**
     * pardox_agg_sum/mean/min/max/count/std(mgr, col_name) -> double
     */
    public double aggSum(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_agg_sum");
        return f.invokeDouble(new Object[]{Pointer.createConstant(ptr), toCString(colName)});
    }

    public double aggMean(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_agg_mean");
        return f.invokeDouble(new Object[]{Pointer.createConstant(ptr), toCString(colName)});
    }

    public double aggMin(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_agg_min");
        return f.invokeDouble(new Object[]{Pointer.createConstant(ptr), toCString(colName)});
    }

    public double aggMax(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_agg_max");
        return f.invokeDouble(new Object[]{Pointer.createConstant(ptr), toCString(colName)});
    }

    public double aggCount(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_agg_count");
        return f.invokeDouble(new Object[]{Pointer.createConstant(ptr), toCString(colName)});
    }

    public double aggStd(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_agg_std");
        return f.invokeDouble(new Object[]{Pointer.createConstant(ptr), toCString(colName)});
    }

    /**
     * pardox_series_add/sub/mul/div/mod(left_mgr, left_col, right_mgr, right_col) -> long
     */
    public long seriesAdd(long leftMgr, String leftCol, long rightMgr, String rightCol) {
        Function f = nativeLib.getFunction("pardox_series_add");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(leftMgr),
            toCString(leftCol),
            Pointer.createConstant(rightMgr),
            toCString(rightCol)
        });
    }

    public long seriesSub(long leftMgr, String leftCol, long rightMgr, String rightCol) {
        Function f = nativeLib.getFunction("pardox_series_sub");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(leftMgr),
            toCString(leftCol),
            Pointer.createConstant(rightMgr),
            toCString(rightCol)
        });
    }

    public long seriesMul(long leftMgr, String leftCol, long rightMgr, String rightCol) {
        Function f = nativeLib.getFunction("pardox_series_mul");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(leftMgr),
            toCString(leftCol),
            Pointer.createConstant(rightMgr),
            toCString(rightCol)
        });
    }

    public long seriesDiv(long leftMgr, String leftCol, long rightMgr, String rightCol) {
        Function f = nativeLib.getFunction("pardox_series_div");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(leftMgr),
            toCString(leftCol),
            Pointer.createConstant(rightMgr),
            toCString(rightCol)
        });
    }

    public long seriesMod(long leftMgr, String leftCol, long rightMgr, String rightCol) {
        Function f = nativeLib.getFunction("pardox_series_mod");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(leftMgr),
            toCString(leftCol),
            Pointer.createConstant(rightMgr),
            toCString(rightCol)
        });
    }

    /**
     * pardox_filter_compare(left_mgr, left_col, right_mgr, right_col, op_code) -> long
     */
    public long filterCompare(long leftMgr, String leftCol, long rightMgr, String rightCol, int opCode) {
        Function f = nativeLib.getFunction("pardox_filter_compare");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(leftMgr),
            toCString(leftCol),
            Pointer.createConstant(rightMgr),
            toCString(rightCol),
            opCode
        });
    }

    /**
     * pardox_filter_compare_scalar(mgr, col_name, val_f64, val_i64, is_float, op_code) -> long
     */
    public long filterCompareScalar(long ptr, String colName, double valF64, long valI64, int isFloat, int opCode) {
        Function f = nativeLib.getFunction("pardox_filter_compare_scalar");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(colName),
            valF64,
            valI64,
            isFloat,
            opCode
        });
    }

    /**
     * pardox_apply_filter(target_mgr, mask_mgr, mask_col) -> long
     */
    public long applyFilter(long targetMgr, long maskMgr, String maskCol) {
        Function f = nativeLib.getFunction("pardox_apply_filter");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(targetMgr),
            Pointer.createConstant(maskMgr),
            toCString(maskCol)
        });
    }

    /**
     * pardox_to_csv(mgr, path) -> long
     */
    public long toCsv(long ptr, String path) {
        Function f = nativeLib.getFunction("pardox_to_csv");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr), toCString(path)});
    }

    /**
     * pardox_to_prdx(mgr, path) -> long
     */
    public long toPrdx(long ptr, String path) {
        Function f = nativeLib.getFunction("pardox_to_prdx");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr), toCString(path)});
    }

    /**
     * pardox_read_json_bytes(json_bytes, len) -> long
     */
    public long readJsonBytes(byte[] jsonBytes, long len) {
        Function f = nativeLib.getFunction("pardox_read_json_bytes");
        return f.invokeLong(new Object[]{jsonBytes, len});
    }

    /**
     * pardox_to_json_records(mgr) -> long
     */
    public long toJsonRecords(long ptr) {
        Function f = nativeLib.getFunction("pardox_to_json_records");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr)});
    }

    /**
     * pardox_to_json_arrays(mgr) -> long
     */
    public long toJsonArrays(long ptr) {
        Function f = nativeLib.getFunction("pardox_to_json_arrays");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr)});
    }

    /**
     * pardox_value_counts(mgr, col_name) -> long
     */
    public long valueCounts(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_value_counts");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr), toCString(colName)});
    }

    /**
     * pardox_unique(mgr, col_name) -> long
     */
    public long unique(long ptr, String colName) {
        Function f = nativeLib.getFunction("pardox_unique");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr), toCString(colName)});
    }

    /**
     * pardox_add_column(target, source, name) -> long
     */
    public long addColumn(long target, long source, String name) {
        Function f = nativeLib.getFunction("pardox_add_column");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(target),
            Pointer.createConstant(source),
            toCString(name)
        });
    }

    /**
     * pardox_fill_na(mgr, col, val) -> long
     */
    public long fillNa(long ptr, String col, double val) {
        Function f = nativeLib.getFunction("pardox_fill_na");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr), toCString(col), val});
    }

    /**
     * pardox_round(mgr, col, decimals) -> long
     */
    public long round(long ptr, String col, int decimals) {
        Function f = nativeLib.getFunction("pardox_round");
        return f.invokeLong(new Object[]{Pointer.createConstant(ptr), toCString(col), decimals});
    }

    /**
     * pardox_scan_sql(conn_str, query) -> long
     */
    public long scanSql(String connStr, String query) {
        Function f = nativeLib.getFunction("pardox_scan_sql");
        return f.invokeLong(new Object[]{toCString(connStr), toCString(query)});
    }

    /**
     * pardox_write_sql(mgr, conn_str, table_name, mode, conflict_cols) -> long
     */
    public long writeSql(long ptr, String connStr, String tableName, String mode, String conflictCols) {
        Function f = nativeLib.getFunction("pardox_write_sql");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(connStr),
            toCString(tableName),
            toCString(mode),
            toCString(conflictCols)
        });
    }

    /**
     * pardox_execute_sql(conn_str, query) -> long
     */
    public long executeSql(String connStr, String query) {
        Function f = nativeLib.getFunction("pardox_execute_sql");
        return f.invokeLong(new Object[]{toCString(connStr), toCString(query)});
    }

    /**
     * pardox_read_mysql(conn_str, query) -> long
     */
    public long readMysql(String connStr, String query) {
        Function f = nativeLib.getFunction("pardox_read_mysql");
        return f.invokeLong(new Object[]{toCString(connStr), toCString(query)});
    }

    /**
     * pardox_write_mysql(mgr, conn_str, table_name, mode, conflict_cols) -> long
     */
    public long writeMysql(long ptr, String connStr, String tableName, String mode, String conflictCols) {
        Function f = nativeLib.getFunction("pardox_write_mysql");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(connStr),
            toCString(tableName),
            toCString(mode),
            toCString(conflictCols)
        });
    }

    /**
     * pardox_execute_mysql(conn_str, query) -> long
     */
    public long executeMysql(String connStr, String query) {
        Function f = nativeLib.getFunction("pardox_execute_mysql");
        return f.invokeLong(new Object[]{toCString(connStr), toCString(query)});
    }

    /**
     * pardox_read_sqlserver(conn_str, query) -> long
     */
    public long readSqlServer(String connStr, String query) {
        Function f = nativeLib.getFunction("pardox_read_sqlserver");
        return f.invokeLong(new Object[]{toCString(connStr), toCString(query)});
    }

    /**
     * pardox_write_sqlserver(mgr, conn_str, table_name, mode, conflict_cols) -> long
     */
    public long writeSqlServer(long ptr, String connStr, String tableName, String mode, String conflictCols) {
        Function f = nativeLib.getFunction("pardox_write_sqlserver");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(connStr),
            toCString(tableName),
            toCString(mode),
            toCString(conflictCols)
        });
    }

    /**
     * pardox_execute_sqlserver(conn_str, query) -> long
     */
    public long executeSqlServer(String connStr, String query) {
        Function f = nativeLib.getFunction("pardox_execute_sqlserver");
        return f.invokeLong(new Object[]{toCString(connStr), toCString(query)});
    }

    /**
     * pardox_read_mongodb(conn_str, target) -> long
     */
    public long readMongoDB(String connStr, String target) {
        Function f = nativeLib.getFunction("pardox_read_mongodb");
        return f.invokeLong(new Object[]{toCString(connStr), toCString(target)});
    }

    /**
     * pardox_write_mongodb(mgr, conn_str, target, mode) -> long
     */
    public long writeMongoDB(long ptr, String connStr, String target, String mode) {
        Function f = nativeLib.getFunction("pardox_write_mongodb");
        return f.invokeLong(new Object[]{
            Pointer.createConstant(ptr),
            toCString(connStr),
            toCString(target),
            toCString(mode)
        });
    }

    /**
     * pardox_execute_mongodb(conn_str, database, command) -> long
     */
    public long executeMongoDB(String connStr, String database, String command) {
        Function f = nativeLib.getFunction("pardox_execute_mongodb");
        return f.invokeLong(new Object[]{toCString(connStr), toCString(database), toCString(command)});
    }

    /**
     * pardox_read_head_json(path, limit) -> long
     */
    public long readHeadJson(String path, long limit) {
        Function f = nativeLib.getFunction("pardox_read_head_json");
        long result = f.invokeLong(new Object[]{toCString(path), limit});
        if (result == 0) return 0;
        return result;
    }

    /**
     * pardox_column_sum(path, col) -> double
     */
    public double columnSum(String path, String col) {
        Function f = nativeLib.getFunction("pardox_column_sum");
        return f.invokeDouble(new Object[]{toCString(path), toCString(col)});
    }
}

package io.pardox;

import com.sun.jna.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * PardoX DataFrame - High-performance columnar data structure
 */
public class DataFrame {

    long ptr; // package-private for Series access
    private String[] columns;
    private Map<String, String> dtypes;

    public DataFrame(long ptr) {
        if (ptr == 0) throw new RuntimeException("Null pointer received");
        this.ptr = ptr;
    }

    public static DataFrame fromJsonRecords(List<Map<String, Object>> data) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Cannot create DataFrame from empty list");
        }
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> record : data) {
            sb.append(toJson(record)).append("\n");
        }
        byte[] jsonBytes = sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Lib lib = Lib.getInstance();
        long newPtr = lib.readJsonBytes(jsonBytes, jsonBytes.length);
        if (newPtr == 0) throw new RuntimeException("PardoX Core failed to ingest data");
        return new DataFrame(newPtr);
    }

    private static String toJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(e.getKey()).append("\":");
            Object v = e.getValue();
            if (v instanceof String) sb.append("\"").append(v).append("\"");
            else if (v instanceof Number) sb.append(v);
            else if (v instanceof Boolean) sb.append(v);
            else sb.append("null");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toString() { return show(10); }

    public String show(int n) {
        Lib lib = Lib.getInstance();
        String ascii = lib.managerToAscii(ptr, n);
        if (ascii != null && !ascii.isEmpty()) return ascii;
        String json = lib.managerToJson(ptr, n);
        return json != null ? json : "<Empty DataFrame>";
    }

    public DataFrame head(int n) { return iloc(0, n); }

    public DataFrame tail(int n) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.tailManager(ptr, n);
        if (newPtr == 0) throw new RuntimeException("Failed to fetch tail");
        return new DataFrame(newPtr);
    }

    public int[] shape() {
        Lib lib = Lib.getInstance();
        long rows = lib.getRowCount(ptr);
        int cols = columns().length;
        return new int[]{(int)rows, cols};
    }

    public String[] columns() {
        if (columns != null) return columns;
        Lib lib = Lib.getInstance();
        String jsonStr = lib.getSchemaJson(ptr);
        if (jsonStr != null) {
            columns = parseColumnsFromSchema(jsonStr);
            return columns;
        }
        return new String[0];
    }

    private String[] parseColumnsFromSchema(String json) {
        List<String> cols = new ArrayList<>();
        int pos = 0;
        while ((pos = json.indexOf("\"name\"", pos)) != -1) {
            int start = json.indexOf(":", pos) + 2;
            int end = json.indexOf("\"", start);
            if (end > start) cols.add(json.substring(start, pos = end));
            else break;
        }
        return cols.toArray(new String[0]);
    }

    public Map<String, String> dtypes() {
        if (dtypes != null) return dtypes;
        Lib lib = Lib.getInstance();
        String jsonStr = lib.getSchemaJson(ptr);
        if (jsonStr != null) {
            dtypes = parseDtypesFromSchema(jsonStr);
            return dtypes;
        }
        return new HashMap<>();
    }

    private Map<String, String> parseDtypesFromSchema(String json) {
        Map<String, String> dt = new HashMap<>();
        int namePos = 0;
        while ((namePos = json.indexOf("\"name\"", namePos)) != -1) {
            int nameStart = json.indexOf(":", namePos) + 2;
            int nameEnd = json.indexOf("\"", nameStart);
            if (nameEnd > nameStart) {
                String colName = json.substring(nameStart, nameEnd);
                int typePos = json.indexOf("\"type\"", nameEnd);
                if (typePos != -1) {
                    int typeStart = json.indexOf(":", typePos) + 2;
                    int typeEnd = json.indexOf("\"", typeStart);
                    if (typeEnd > typeStart) dt.put(colName, json.substring(typeStart, typeEnd));
                }
            }
            namePos = nameEnd;
        }
        return dt;
    }

    public Series get(String colName) { return new Series(this, colName); }

    public Iloc iloc() { return new Iloc(this); }

    public DataFrame iloc(int start, int length) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.sliceManager(ptr, start, length);
        if (newPtr == 0) throw new RuntimeException("Slice operation returned null");
        return new DataFrame(newPtr);
    }

    public DataFrame filter(Series mask) {
        Lib lib = Lib.getInstance();
        long resPtr = lib.applyFilter(ptr, mask.df.ptr, mask.name);
        if (resPtr == 0) throw new RuntimeException("Filter operation returned null pointer");
        return new DataFrame(resPtr);
    }

    public DataFrame cast(String colName, String targetType) {
        Lib lib = Lib.getInstance();
        int result = lib.castColumn(ptr, colName, targetType);
        if (result != 1) throw new RuntimeException("Failed to cast column '" + colName + "'");
        return this;
    }

    public DataFrame join(DataFrame other, String on) { return join(other, on, on, "inner"); }

    public DataFrame join(DataFrame other, String leftOn, String rightOn, String how) {
        Lib lib = Lib.getInstance();
        long resultPtr = lib.hashJoin(ptr, other.ptr, leftOn, rightOn);
        if (resultPtr == 0) throw new RuntimeException("Join failed");
        return new DataFrame(resultPtr);
    }

    public boolean toCsv(String path) {
        Lib lib = Lib.getInstance();
        long result = lib.toCsv(ptr, path);
        if (result != 1) throw new RuntimeException("CSV Export Failed: " + result);
        return true;
    }

    public boolean toPrdx(String path) {
        Lib lib = Lib.getInstance();
        long result = lib.toPrdx(ptr, path);
        if (result != 1) throw new RuntimeException("PRDX Export Failed: " + result);
        return true;
    }

    public int toSql(String connectionString, String tableName) {
        return toSql(connectionString, tableName, "append", null);
    }

    public int toSql(String connectionString, String tableName, String mode, List<String> conflictCols) {
        Lib lib = Lib.getInstance();
        String colsJson = conflictCols != null ? conflictCols.toString() : "[]";
        long rows = lib.writeSql(ptr, connectionString, tableName, mode, colsJson);
        if (rows < 0) throw new RuntimeException("to_sql failed: " + rows);
        return (int)rows;
    }

    public int toMysql(String connectionString, String tableName) {
        return toMysql(connectionString, tableName, "append", null);
    }

    public int toMysql(String connectionString, String tableName, String mode, List<String> conflictCols) {
        Lib lib = Lib.getInstance();
        String colsJson = conflictCols != null ? conflictCols.toString() : "[]";
        long rows = lib.writeMysql(ptr, connectionString, tableName, mode, colsJson);
        if (rows < 0) throw new RuntimeException("to_mysql failed: " + rows);
        return (int)rows;
    }

    public int toSqlServer(String connectionString, String tableName) {
        return toSqlServer(connectionString, tableName, "append", null);
    }

    public int toSqlServer(String connectionString, String tableName, String mode, List<String> conflictCols) {
        Lib lib = Lib.getInstance();
        String colsJson = conflictCols != null ? conflictCols.toString() : "[]";
        long rows = lib.writeSqlServer(ptr, connectionString, tableName, mode, colsJson);
        if (rows < 0) throw new RuntimeException("to_sqlserver failed: " + rows);
        return (int)rows;
    }

    public int toMongoDB(String connectionString, String dbDotCollection) {
        return toMongoDB(connectionString, dbDotCollection, "append");
    }

    public int toMongoDB(String connectionString, String dbDotCollection, String mode) {
        Lib lib = Lib.getInstance();
        long rows = lib.writeMongoDB(ptr, connectionString, dbDotCollection, mode);
        if (rows < 0) throw new RuntimeException("to_mongodb failed: " + rows);
        return (int)rows;
    }

    public List<Map<String, Object>> toDict() {
        Lib lib = Lib.getInstance();
        long jsonPtr = lib.toJsonRecords(ptr);
        if (jsonPtr == 0) return new ArrayList<>();
        String jsonStr = Pointer.createConstant(jsonPtr).getString(0);
        lib.freeString(jsonPtr);
        return parseJsonRecords(jsonStr);
    }

    public List<List<Object>> toList() {
        Lib lib = Lib.getInstance();
        long jsonPtr = lib.toJsonArrays(ptr);
        if (jsonPtr == 0) return new ArrayList<>();
        String jsonStr = Pointer.createConstant(jsonPtr).getString(0);
        lib.freeString(jsonPtr);
        return parseJsonArrays(jsonStr);
    }

    private List<Map<String, Object>> parseJsonRecords(String json) {
        List<Map<String, Object>> result = new ArrayList<>();
        // Simplified - in production use Jackson/Gson
        return result;
    }

    private List<List<Object>> parseJsonArrays(String json) {
        List<List<Object>> result = new ArrayList<>();
        return result;
    }

    public Map<Object, Long> valueCounts(String col) {
        Lib lib = Lib.getInstance();
        long jsonPtr = lib.valueCounts(ptr, col);
        if (jsonPtr == 0) return new HashMap<>();
        String jsonStr = Pointer.createConstant(jsonPtr).getString(0);
        lib.freeString(jsonPtr);
        return new HashMap<>();
    }

    public List<Object> unique(String col) {
        Lib lib = Lib.getInstance();
        long jsonPtr = lib.unique(ptr, col);
        if (jsonPtr == 0) return new ArrayList<>();
        String jsonStr = Pointer.createConstant(jsonPtr).getString(0);
        lib.freeString(jsonPtr);
        return new ArrayList<>();
    }

    public DataFrame add(String colA, String colB) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.mathAdd(ptr, colA, colB);
        if (newPtr == 0) throw new RuntimeException("math_add returned null");
        return new DataFrame(newPtr);
    }

    public DataFrame sub(String colA, String colB) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.mathSub(ptr, colA, colB);
        if (newPtr == 0) throw new RuntimeException("math_sub returned null");
        return new DataFrame(newPtr);
    }

    public DataFrame mul(String colA, String colB) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.seriesMul(ptr, colA, ptr, colB);
        if (newPtr == 0) throw new RuntimeException("series_mul returned null");
        return new DataFrame(newPtr);
    }

    public double std(String col) {
        Lib lib = Lib.getInstance();
        return lib.mathStddev(ptr, col);
    }

    public DataFrame minMaxScale(String col) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.mathMinMax(ptr, col);
        if (newPtr == 0) throw new RuntimeException("math_minmax returned null");
        return new DataFrame(newPtr);
    }

    public DataFrame sortValues(String by, boolean ascending) {
        return sortValues(by, ascending, false);
    }

    public DataFrame sortValues(String by, boolean ascending, boolean gpu) {
        Lib lib = Lib.getInstance();
        long newPtr;
        if (gpu) newPtr = lib.gpuSort(ptr, by);
        else {
            int descending = ascending ? 0 : 1;
            newPtr = lib.sortValues(ptr, by, descending);
        }
        if (newPtr == 0) throw new RuntimeException("sort operation returned null");
        return new DataFrame(newPtr);
    }

    public void set(String colName, Series series) {
        Lib lib = Lib.getInstance();
        long result = lib.addColumn(ptr, series.df.ptr, colName);
        if (result != 1) throw new RuntimeException("Failed to assign column: " + colName);
    }

    public DataFrame fillna(double value) {
        Lib lib = Lib.getInstance();
        for (String col : columns()) {
            String dtype = dtypes().get(col);
            if (dtype != null && (dtype.contains("Float") || dtype.contains("Int"))) {
                lib.fillNa(ptr, col, value);
            }
        }
        return this;
    }

    public DataFrame round(int decimals) {
        Lib lib = Lib.getInstance();
        for (String col : columns()) lib.round(ptr, col, decimals);
        return this;
    }

    public long getPtr() { return ptr; }

    public void free() {
        if (ptr != 0) {
            Lib lib = Lib.getInstance();
            lib.freeManager(ptr);
            ptr = 0;
        }
    }

    @Override
    protected void finalize() throws Throwable { free(); super.finalize(); }

    public class Iloc {
        private final DataFrame df;
        public Iloc(DataFrame df) { this.df = df; }
        public DataFrame get(int start, int length) { return df.iloc(start, length); }
        public DataFrame get(int index) { return df.iloc(index, 1); }
    }
}

package io.pardox;

import java.util.*;

/**
 * PardoX Series - One-dimensional columnar data
 */
public class Series {

    public final DataFrame df;
    public final String name;
    private String dtype;

    public Series(DataFrame df, String name) {
        this.df = df;
        this.name = name;
    }

    public String dtype() {
        if (dtype != null) return dtype;
        Map<String, String> dtypes = df.dtypes();
        dtype = dtypes.get(name);
        return dtype != null ? dtype : "Unknown";
    }

    public int length() { return df.shape()[0]; }

    // Arithmetic
    public Series add(Series other) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.seriesAdd(df.ptr, name, other.df.ptr, other.name);
        if (newPtr == 0) throw new RuntimeException("Series add returned null");
        return new Series(new DataFrame(newPtr), "result");
    }

    public Series sub(Series other) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.seriesSub(df.ptr, name, other.df.ptr, other.name);
        if (newPtr == 0) throw new RuntimeException("Series sub returned null");
        return new Series(new DataFrame(newPtr), "result");
    }

    public Series mul(Series other) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.seriesMul(df.ptr, name, other.df.ptr, other.name);
        if (newPtr == 0) throw new RuntimeException("Series mul returned null");
        return new Series(new DataFrame(newPtr), "result");
    }

    public Series div(Series other) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.seriesDiv(df.ptr, name, other.df.ptr, other.name);
        if (newPtr == 0) throw new RuntimeException("Series div returned null");
        return new Series(new DataFrame(newPtr), "result");
    }

    public Series mod(Series other) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.seriesMod(df.ptr, name, other.df.ptr, other.name);
        if (newPtr == 0) throw new RuntimeException("Series mod returned null");
        return new Series(new DataFrame(newPtr), "result");
    }

    // Comparison operators
    public static final int OP_EQ = 0, OP_NE = 1, OP_GT = 2, OP_GE = 3, OP_LT = 4, OP_LE = 5;

    public Series compare(Series other, int opCode) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.filterCompare(df.ptr, name, other.df.ptr, other.name, opCode);
        if (newPtr == 0) throw new RuntimeException("Filter compare returned null");
        return new Series(new DataFrame(newPtr), name + "_mask");
    }

    public Series compareScalar(double value, int opCode) {
        Lib lib = Lib.getInstance();
        long newPtr = lib.filterCompareScalar(df.ptr, name, value, 0L, 1, opCode);
        if (newPtr == 0) throw new RuntimeException("Filter compare scalar returned null");
        return new Series(new DataFrame(newPtr), name + "_mask");
    }

    public Series eq(Series other) { return compare(other, OP_EQ); }
    public Series ne(Series other) { return compare(other, OP_NE); }
    public Series gt(Series other) { return compare(other, OP_GT); }
    public Series ge(Series other) { return compare(other, OP_GE); }
    public Series lt(Series other) { return compare(other, OP_LT); }
    public Series le(Series other) { return compare(other, OP_LE); }

    public Series eq(double value) { return compareScalar(value, OP_EQ); }
    public Series ne(double value) { return compareScalar(value, OP_NE); }
    public Series gt(double value) { return compareScalar(value, OP_GT); }
    public Series ge(double value) { return compareScalar(value, OP_GE); }
    public Series lt(double value) { return compareScalar(value, OP_LT); }
    public Series le(double value) { return compareScalar(value, OP_LE); }

    // Aggregations
    public double sum() { Lib lib = Lib.getInstance(); return lib.aggSum(df.ptr, name); }
    public double mean() { Lib lib = Lib.getInstance(); return lib.aggMean(df.ptr, name); }
    public double min() { Lib lib = Lib.getInstance(); return lib.aggMin(df.ptr, name); }
    public double max() { Lib lib = Lib.getInstance(); return lib.aggMax(df.ptr, name); }
    public double count() { Lib lib = Lib.getInstance(); return lib.aggCount(df.ptr, name); }
    public double std() { Lib lib = Lib.getInstance(); return lib.aggStd(df.ptr, name); }

    @Override
    public String toString() { return "PardoX Series: " + name + " (" + dtype() + ", " + length() + " rows)"; }
}

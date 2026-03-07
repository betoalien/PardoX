package io.pardox;

/**
 * PardoX - High-Performance DataFrame Engine
 *
 * A universal DataFrame library powered by Rust with zero dependencies.
 *
 * Example usage:
 * <pre>
 * import io.pardox.*;
 *
 * // Read CSV
 * DataFrame df = Io.readCsv("data.csv");
 *
 * // Show data
 * System.out.println(df.show(10));
 *
 * // Get shape
 * int[] shape = df.shape();
 *
 * // Filter
 * Series mask = df.get("price").gt(100.0);
 * DataFrame filtered = df.filter(mask);
 *
 * // Write to PostgreSQL
 * filtered.toSql("postgresql://user:pass@localhost/db", "table_name");
 * </pre>
 *
 * Supported platforms:
 * - Linux x64 (.so)
 * - Windows x64 (.dll)
 * - macOS Intel (.dylib)
 * - macOS ARM64 (.dylib)
 *
 * @version 0.3.2
 * @author PardoX Team
 */
public class Pardox {

    /**
     * Initialize the PardoX engine
     */
    static {
        try {
            // Load and initialize the native library
            Lib lib = Lib.getInstance();

            // Try to initialize the engine
            lib.initEngine();

            System.out.println("PardoX v0.3.2 loaded successfully");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Failed to load PardoX native library: " + e.getMessage());
        }
    }

    /**
     * Prevent instantiation
     */
    private Pardox() {}

    /**
     * Get version info
     */
    public static String version() {
        return "0.3.2";
    }
}

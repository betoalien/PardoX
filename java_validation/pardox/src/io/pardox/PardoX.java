package io.pardox;

/**
 * PardoX - High-Performance Universal DataFrame Engine
 *
 * A zero-dependency DataFrame engine with native bindings for multiple languages.
 * Built in Rust with bindings for Python, PHP, JavaScript, and Java.
 *
 * Quick Start:
 * <pre>
 * import io.pardox.*;
 *
 * // Read CSV
 * DataFrame df = Io.readCsv("data.csv");
 *
 * // Show data
 * df.show();
 *
 * // Get column
 * Series col = df.get("sales");
 *
 * // Write to database
 * df.toSql("postgresql://user:pass@localhost/db", "table_name");
 * </pre>
 *
 * @version 0.3.2
 * @author PardoX Team
 */
public class PardoX {

    /**
     * Initialize the PardoX engine
     */
    public static void init() {
        Io.init();
    }

    /**
     * Get version string
     */
    public static String version() {
        return "0.3.2";
    }
}

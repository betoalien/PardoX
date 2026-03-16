/* tslint:disable */
/* eslint-disable */

/**
 * PardoX WebAssembly DataFrame handle.
 */
export class PardoxWasm {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Number of columns.
     */
    col_count(): number;
    /**
     * Column names as a JSON array string.
     */
    column_names_json(): string;
    /**
     * Load CSV (comma-delimited).
     */
    static from_csv(csv: string): PardoxWasm;
    /**
     * Load CSV with a custom single-character delimiter.
     */
    static from_csv_with_sep(csv: string, sep: string): PardoxWasm;
    /**
     * Reconstruct a frame from JSON previously produced by `to_storage_json`.
     */
    static from_storage_json(json_str: string): PardoxWasm;
    /**
     * Get a Float64 column as a JSON number array (null for NaN/missing).
     * Returns "null" if the column does not exist or is not numeric.
     */
    get_column_f64_json(col: string): string;
    /**
     * Get a Utf8 column as a JSON string array.
     * Returns "null" if the column does not exist or is not a string column.
     */
    get_column_str_json(col: string): string;
    /**
     * Number of rows.
     */
    row_count(): number;
    /**
     * Schema as JSON: [{name, type}, …]
     */
    schema_json(): string;
    /**
     * Execute a SQL SELECT query over this frame, returning a new frame.
     */
    sql(query: string): PardoxWasm;
    /**
     * Serialize to JSON arrays format: {columns:[…], data:[[…],…], nrows:N}
     */
    to_json_arrays(): string;
    /**
     * Serialize to JSON records format: [{col: val, …}, …]
     */
    to_json_records(): string;
    /**
     * Serialize the frame to a JSON string suitable for IndexedDB storage.
     */
    to_storage_json(): string;
}

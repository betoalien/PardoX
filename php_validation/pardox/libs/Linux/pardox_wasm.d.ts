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

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_pardoxwasm_free: (a: number, b: number) => void;
    readonly pardoxwasm_col_count: (a: number) => number;
    readonly pardoxwasm_column_names_json: (a: number) => [number, number];
    readonly pardoxwasm_from_csv: (a: number, b: number) => number;
    readonly pardoxwasm_from_csv_with_sep: (a: number, b: number, c: number, d: number) => number;
    readonly pardoxwasm_from_storage_json: (a: number, b: number) => [number, number, number];
    readonly pardoxwasm_get_column_f64_json: (a: number, b: number, c: number) => [number, number];
    readonly pardoxwasm_get_column_str_json: (a: number, b: number, c: number) => [number, number];
    readonly pardoxwasm_row_count: (a: number) => number;
    readonly pardoxwasm_schema_json: (a: number) => [number, number];
    readonly pardoxwasm_sql: (a: number, b: number, c: number) => [number, number, number];
    readonly pardoxwasm_to_json_arrays: (a: number) => [number, number];
    readonly pardoxwasm_to_json_records: (a: number) => [number, number];
    readonly pardoxwasm_to_storage_json: (a: number) => [number, number];
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;

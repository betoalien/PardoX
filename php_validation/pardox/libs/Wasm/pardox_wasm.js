/* @ts-self-types="./pardox_wasm.d.ts" */

/**
 * PardoX WebAssembly DataFrame handle.
 */
class PardoxWasm {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(PardoxWasm.prototype);
        obj.__wbg_ptr = ptr;
        PardoxWasmFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        PardoxWasmFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_pardoxwasm_free(ptr, 0);
    }
    /**
     * Number of columns.
     * @returns {number}
     */
    col_count() {
        const ret = wasm.pardoxwasm_col_count(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Column names as a JSON array string.
     * @returns {string}
     */
    column_names_json() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.pardoxwasm_column_names_json(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Load CSV (comma-delimited).
     * @param {string} csv
     * @returns {PardoxWasm}
     */
    static from_csv(csv) {
        const ptr0 = passStringToWasm0(csv, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.pardoxwasm_from_csv(ptr0, len0);
        return PardoxWasm.__wrap(ret);
    }
    /**
     * Load CSV with a custom single-character delimiter.
     * @param {string} csv
     * @param {string} sep
     * @returns {PardoxWasm}
     */
    static from_csv_with_sep(csv, sep) {
        const ptr0 = passStringToWasm0(csv, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(sep, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.pardoxwasm_from_csv_with_sep(ptr0, len0, ptr1, len1);
        return PardoxWasm.__wrap(ret);
    }
    /**
     * Reconstruct a frame from JSON previously produced by `to_storage_json`.
     * @param {string} json_str
     * @returns {PardoxWasm}
     */
    static from_storage_json(json_str) {
        const ptr0 = passStringToWasm0(json_str, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.pardoxwasm_from_storage_json(ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return PardoxWasm.__wrap(ret[0]);
    }
    /**
     * Get a Float64 column as a JSON number array (null for NaN/missing).
     * Returns "null" if the column does not exist or is not numeric.
     * @param {string} col
     * @returns {string}
     */
    get_column_f64_json(col) {
        let deferred2_0;
        let deferred2_1;
        try {
            const ptr0 = passStringToWasm0(col, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len0 = WASM_VECTOR_LEN;
            const ret = wasm.pardoxwasm_get_column_f64_json(this.__wbg_ptr, ptr0, len0);
            deferred2_0 = ret[0];
            deferred2_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * Get a Utf8 column as a JSON string array.
     * Returns "null" if the column does not exist or is not a string column.
     * @param {string} col
     * @returns {string}
     */
    get_column_str_json(col) {
        let deferred2_0;
        let deferred2_1;
        try {
            const ptr0 = passStringToWasm0(col, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len0 = WASM_VECTOR_LEN;
            const ret = wasm.pardoxwasm_get_column_str_json(this.__wbg_ptr, ptr0, len0);
            deferred2_0 = ret[0];
            deferred2_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
        }
    }
    /**
     * Number of rows.
     * @returns {number}
     */
    row_count() {
        const ret = wasm.pardoxwasm_row_count(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Schema as JSON: [{name, type}, …]
     * @returns {string}
     */
    schema_json() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.pardoxwasm_schema_json(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Execute a SQL SELECT query over this frame, returning a new frame.
     * @param {string} query
     * @returns {PardoxWasm}
     */
    sql(query) {
        const ptr0 = passStringToWasm0(query, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.pardoxwasm_sql(this.__wbg_ptr, ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return PardoxWasm.__wrap(ret[0]);
    }
    /**
     * Serialize to JSON arrays format: {columns:[…], data:[[…],…], nrows:N}
     * @returns {string}
     */
    to_json_arrays() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.pardoxwasm_to_json_arrays(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Serialize to JSON records format: [{col: val, …}, …]
     * @returns {string}
     */
    to_json_records() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.pardoxwasm_to_json_records(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Serialize the frame to a JSON string suitable for IndexedDB storage.
     * @returns {string}
     */
    to_storage_json() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.pardoxwasm_to_storage_json(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
}
if (Symbol.dispose) PardoxWasm.prototype[Symbol.dispose] = PardoxWasm.prototype.free;
exports.PardoxWasm = PardoxWasm;

function __wbg_get_imports() {
    const import0 = {
        __proto__: null,
        __wbg___wbindgen_throw_6ddd609b62940d55: function(arg0, arg1) {
            throw new Error(getStringFromWasm0(arg0, arg1));
        },
        __wbindgen_cast_0000000000000001: function(arg0, arg1) {
            // Cast intrinsic for `Ref(String) -> Externref`.
            const ret = getStringFromWasm0(arg0, arg1);
            return ret;
        },
        __wbindgen_init_externref_table: function() {
            const table = wasm.__wbindgen_externrefs;
            const offset = table.grow(4);
            table.set(0, undefined);
            table.set(offset + 0, undefined);
            table.set(offset + 1, null);
            table.set(offset + 2, true);
            table.set(offset + 3, false);
        },
    };
    return {
        __proto__: null,
        "./pardox_wasm_bg.js": import0,
    };
}

const PardoxWasmFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_pardoxwasm_free(ptr >>> 0, 1));

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return decodeText(ptr, len);
}

let cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

function passStringToWasm0(arg, malloc, realloc) {
    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }
    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = cachedTextEncoder.encodeInto(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function takeFromExternrefTable0(idx) {
    const value = wasm.__wbindgen_externrefs.get(idx);
    wasm.__externref_table_dealloc(idx);
    return value;
}

let cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
function decodeText(ptr, len) {
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

const cachedTextEncoder = new TextEncoder();

if (!('encodeInto' in cachedTextEncoder)) {
    cachedTextEncoder.encodeInto = function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
            read: arg.length,
            written: buf.length
        };
    };
}

let WASM_VECTOR_LEN = 0;

const wasmPath = `${__dirname}/pardox_wasm_bg.wasm`;
const wasmBytes = require('fs').readFileSync(wasmPath);
const wasmModule = new WebAssembly.Module(wasmBytes);
let wasm = new WebAssembly.Instance(wasmModule, __wbg_get_imports()).exports;
wasm.__wbindgen_start();

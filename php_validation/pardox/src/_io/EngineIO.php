<?php

namespace PardoX\IOOps;

trait EngineIOTrait
{
    // =========================================================================
    // ENGINE UTILITIES
    // =========================================================================

    /**
     * Reset the PardoX global engine state.
     */
    public static function reset(): void
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ffi->pardox_reset();
    }

    /**
     * Send a legacy ping to verify the engine is alive.
     *
     * @param string $systemName System identifier string.
     * @return int Response code.
     */
    public static function ping(string $systemName = 'pardox'): int
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (int) $ffi->pardox_legacy_ping($systemName);
    }

    /**
     * Return the PardoX Core library version string.
     *
     * @return string Version string.
     */
    public static function engineVersion(): string
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_version();
        return $result ?? '';
    }

    /**
     * Return a system report from the PardoX engine.
     *
     * @return array System info dict.
     */
    public static function systemReport(): array
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_get_system_report();
        return $result ? json_decode($result, true) : [];
    }

    /**
     * Return quarantine log entries from the engine.
     *
     * @return array Log entries.
     */
    public static function getQuarantineLogs(): array
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $result = $ffi->pardox_get_quarantine_logs();
        return $result ? json_decode($result, true) : [];
    }

    /**
     * Clear all quarantine log entries.
     */
    public static function clearQuarantine(): void
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        $ffi->pardox_clear_quarantine();
    }

    /**
     * Check whether SQL Server driver is correctly configured.
     *
     * @return bool True if configured.
     */
    public static function sqlserverConfigOk(): bool
    {
        $ffi = \PardoX\Core\FFI::getInstance();
        return (bool) $ffi->pardox_sqlserver_config_ok();
    }
}

<?php

namespace PardoX\Core;

use RuntimeException;

class Lib
{
    /**
     * Resolves the absolute path to the PardoX binary library based on the OS.
     *
     * @return string Absolute path to the .dll, .so, or .dylib file.
     * @throws RuntimeException If the OS is unsupported or the binary is missing.
     */
    public static function getLibraryPath(): string
    {
        // 1. Detect Operating System
        $os = PHP_OS_FAMILY;
        $libName = '';
        $folder = '';

        switch ($os) {
            case 'Windows':
                $libName = 'pardox-cpu-Windows-x64.dll';
                $folder = 'Win';
                break;

            case 'Linux':
                $libName = 'pardox-cpu-Linux-x64.so';
                $folder = 'Linux';
                break;

            case 'Darwin':
                $folder = 'Mac';
                // Detect Architecture (Apple Silicon vs Intel)
                $arch = strtolower(php_uname('m'));
                
                if (strpos($arch, 'arm64') !== false) {
                    $libName = 'pardox-cpu-MacOS-ARM64.dylib';
                } elseif (strpos($arch, 'x86_64') !== false) {
                    $libName = 'pardox-cpu-MacOS-Intel.dylib';
                } else {
                    throw new RuntimeException("Unsupported MacOS Architecture: " . php_uname('m'));
                }
                break;

            default:
                throw new RuntimeException("Operating system not supported by PardoX: $os");
        }

        // 2. Resolve Path
        // Assumes 'libs' is at the project root (2 levels up from src/Core)
        $baseDir = dirname(__DIR__, 2);
        $libPath = $baseDir . DIRECTORY_SEPARATOR . 'libs' . DIRECTORY_SEPARATOR . $folder . DIRECTORY_SEPARATOR . $libName;

        if (!file_exists($libPath)) {
            throw new RuntimeException("PardoX binary not found at: $libPath");
        }

        return $libPath;
    }
}
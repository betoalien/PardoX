<?php

// check_ffi.php

// 1. Manual Autoload
// Since we haven't set up Composer autoloader yet, we manually require the Lib file.
$libFile = __DIR__ . '/src/Core/Lib.php';

if (!file_exists($libFile)) {
    die("\n[ERROR] Could not find src/Core/Lib.php. Check your directory structure.\n");
}

require_once $libFile;

use PardoX\Core\Lib;

echo "========================================\n";
echo " PardoX Engine - PHP FFI Connection Test\n";
echo "========================================\n";

try {
    // 2. Diagnostics
    echo "[INFO] PHP Version: " . phpversion() . "\n";
    echo "[INFO] OS Family: " . PHP_OS_FAMILY . "\n";
    echo "[INFO] FFI Extension: " . (extension_loaded('ffi') ? "ENABLED" : "DISABLED") . "\n";

    // 3. Attempt to Load the Engine
    echo "[INFO] Attempting to load PardoX Shared Library...\n";
    
    // This calls the Singleton. If it fails, it throws an Exception.
    $ffi = Lib::getInstance();

    echo "\n[SUCCESS] PardoX Engine loaded successfully!\n";
    
    // 4. Inspection
    echo "[INFO] FFI Object Dump:\n";
    var_dump($ffi);

    echo "\n[INFO] Ready to build DataFrame.php wrapper.\n";

} catch (\Exception $e) {
    echo "\n[FATAL ERROR] Connection Failed:\n";
    echo "Reason: " . $e->getMessage() . "\n";
    exit(1);
}
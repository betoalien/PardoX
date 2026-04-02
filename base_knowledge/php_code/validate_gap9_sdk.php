<?php
/**
 * validate_gap9_sdk.php - Gap 9: Time Series Fill (PHP SDK)
 *
 * Tests pardox_ffill, pardox_bfill, pardox_interpolate using:
 *   - Float64 columns with NaN values
 *   - Forward fill, backward fill, linear interpolation
 */

require_once __DIR__ . '/vendor/autoload.php';

use PardoX\DataFrame;

$passed = 0;
$failed = 0;

function section(string $title): void {
    echo "\n" . str_repeat("=", 66) . "\n";
    echo "  $title\n";
    echo str_repeat("=", 66) . "\n";
}

function ok(string $name, string $detail = ""): void {
    global $passed;
    $passed++;
    echo "  [OK]   $name" . ($detail ? " - $detail" : "") . "\n";
}

function fail(string $name, string $detail = ""): void {
    global $failed;
    $failed++;
    echo "  [FAIL] $name" . ($detail ? " - $detail" : "") . "\n";
}

$ffi = \PardoX\Core\FFI::getInstance();

// Helper: get F64 values from a manager
function get_f64_values($mgr, $col, $ffi) {
    $outLen = FFI::new("size_t");
    $ptr = $ffi->pardox_get_f64_buffer($mgr, $col, FFI::addr($outLen));
    if (!$ptr || $outLen->cdata == 0) return null;
    $nrows = $outLen->cdata;
    $arr = FFI::cast("double[$nrows]", $ptr);
    $result = [];
    for ($i = 0; $i < $nrows; $i++) {
        $result[] = $arr[$i];
    }
    return $result;
}

function nrows($mgr, $ffi) {
    return $ffi->pardox_get_row_count($mgr);
}

function col_names($mgr, $ffi) {
    $raw = $ffi->pardox_get_schema_json($mgr);
    if (!$raw) return [];
    $schema = json_decode(FFI::string($raw), true);
    return array_column($schema['columns'] ?? [], 'name');
}

function load_csv($path, $ffi) {
    $config = json_encode(["delimiter" => ",", "has_header" => true]);
    return $ffi->pardox_load_manager_csv($path, "{}", $config);
}

// =============================================================================
echo "\n[INIT] Starting PardoX PHP SDK - Gap 9 validation...";

// =============================================================================
// Section 1: ffill (Forward Fill)
// =============================================================================
section("ffill (Forward Fill)");

// Series: [1, NaN, NaN, 4, NaN, 6] -> After ffill: [1, 1, 1, 4, 4, 6]
$ffillCsv = '_gap9_ffill.csv';
$fp = fopen($ffillCsv, 'w');
fwrite($fp, "ts,val\n");
fwrite($fp, "0,1.0\n");
fwrite($fp, "1,NaN\n");
fwrite($fp, "2,NaN\n");
fwrite($fp, "3,4.0\n");
fwrite($fp, "4,NaN\n");
fwrite($fp, "5,6.0\n");
fclose($fp);

$mgrFf = load_csv($ffillCsv, $ffi);
$rFf = $ffi->pardox_ffill($mgrFf, "val");

// Test 1: returns non-null
if ($rFf) {
    ok("Test 1 - pardox_ffill returns non-null");
} else {
    fail("Test 1 - ffill returned null");
}

// Test 2: row count preserved
if ($rFf) {
    $rc = nrows($rFf, $ffi);
    if ($rc == 6) {
        ok("Test 2 - row count preserved (6)", "got $rc");
    } else {
        fail("Test 2 - row count", "expected 6, got $rc");
    }
}

// Test 3: column name preserved
if ($rFf) {
    $cols = col_names($rFf, $ffi);
    if ($cols == ["val"]) {
        ok("Test 3 - column name is 'val'");
    } else {
        fail("Test 3 - column name", "got ".json_encode($cols));
    }
}

// Test 4: values correctly forward-filled
if ($rFf) {
    $vals = get_f64_values($rFf, "val", $ffi);
    $expected = [1.0, 1.0, 1.0, 4.0, 4.0, 6.0];
    $match = true;
    if (count($vals) == 6) {
        for ($i = 0; $i < 6; $i++) {
            if (abs($vals[$i] - $expected[$i]) > 1e-9) {
                $match = false;
                break;
            }
        }
    } else {
        $match = false;
    }
    if ($match) {
        ok("Test 4 - ffill values correct", json_encode($vals));
    } else {
        fail("Test 4 - ffill values", "expected ".json_encode($expected).", got ".json_encode($vals));
    }
    $ffi->pardox_free_manager($rFf);
}

// Test 5: leading NaN stays NaN
$leadCsv = '_gap9_lead.csv';
$fp = fopen($leadCsv, 'w');
fwrite($fp, "val\n");
fwrite($fp, "NaN\n");
fwrite($fp, "NaN\n");
fwrite($fp, "3.0\n");
fwrite($fp, "4.0\n");
fclose($fp);

$mgrLead = load_csv($leadCsv, $ffi);
$rLead = $ffi->pardox_ffill($mgrLead, "val");
if ($rLead) {
    $v = get_f64_values($rLead, "val", $ffi);
    if ($v && count($v) == 4 && is_nan($v[0]) && is_nan($v[1]) &&
        abs($v[2]-3.0)<1e-9 && abs($v[3]-4.0)<1e-9) {
        ok("Test 5 - leading NaN stays NaN after ffill");
    } else {
        fail("Test 5 - leading NaN", "got ".json_encode($v));
    }
    $ffi->pardox_free_manager($rLead);
} else {
    fail("Test 5 - ffill leading NaN returned null");
}
$ffi->pardox_free_manager($mgrLead);
$ffi->pardox_free_manager($mgrFf);

// =============================================================================
// Section 2: bfill (Backward Fill)
// =============================================================================
section("bfill (Backward Fill)");

// Series: [NaN, 2, NaN, NaN, 5, NaN] -> After bfill: [2, 2, 5, 5, 5, NaN]
$bfillCsv = '_gap9_bfill.csv';
$fp = fopen($bfillCsv, 'w');
fwrite($fp, "ts,val\n");
fwrite($fp, "0,NaN\n");
fwrite($fp, "1,2.0\n");
fwrite($fp, "2,NaN\n");
fwrite($fp, "3,NaN\n");
fwrite($fp, "4,5.0\n");
fwrite($fp, "5,NaN\n");
fclose($fp);

$mgrBf = load_csv($bfillCsv, $ffi);
$rBf = $ffi->pardox_bfill($mgrBf, "val");

// Test 6: returns non-null
if ($rBf) {
    ok("Test 6 - pardox_bfill returns non-null");
} else {
    fail("Test 6 - bfill returned null");
}

// Test 7: values correctly backward-filled
if ($rBf) {
    $valsBf = get_f64_values($rBf, "val", $ffi);
    // Expected: [2, 2, 5, 5, 5, NaN]
    $expectedBf = [2.0, 2.0, 5.0, 5.0, 5.0, NAN];
    $match = true;
    if ($valsBf && count($valsBf) == 6) {
        for ($i = 0; $i < 5; $i++) {
            if (abs($valsBf[$i] - $expectedBf[$i]) > 1e-9) {
                $match = false;
                break;
            }
        }
        if (!is_nan($valsBf[5])) $match = false;
    } else {
        $match = false;
    }
    if ($match) {
        ok("Test 7 - bfill values correct");
    } else {
        fail("Test 7 - bfill values", "expected [2,2,5,5,5,NaN], got ".json_encode($valsBf));
    }
}

// Test 8: trailing NaN stays NaN
if ($rBf) {
    $valsBf = get_f64_values($rBf, "val", $ffi);
    if ($valsBf && is_nan($valsBf[count($valsBf)-1])) {
        ok("Test 8 - trailing NaN stays NaN after bfill");
    } else {
        fail("Test 8 - trailing NaN should remain NaN");
    }
    $ffi->pardox_free_manager($rBf);
}
$ffi->pardox_free_manager($mgrBf);

// =============================================================================
// Section 3: ffill vs bfill combined
// =============================================================================
section("ffill vs bfill combined");

// Series: [NaN, 1, NaN, 3, NaN]
// ffill: [NaN, 1, 1, 3, 3]
// bfill: [1, 1, 3, 3, NaN]
$comboCsv = '_gap9_combo.csv';
$fp = fopen($comboCsv, 'w');
fwrite($fp, "val\n");
fwrite($fp, "NaN\n");
fwrite($fp, "1.0\n");
fwrite($fp, "NaN\n");
fwrite($fp, "3.0\n");
fwrite($fp, "NaN\n");
fclose($fp);

$mgrCombo = load_csv($comboCsv, $ffi);
$rFf2 = $ffi->pardox_ffill($mgrCombo, "val");
$rBf2 = $ffi->pardox_bfill($mgrCombo, "val");

if ($rFf2 && $rBf2) {
    $vf = get_f64_values($rFf2, "val", $ffi);
    $vb = get_f64_values($rBf2, "val", $ffi);

    // ffill check
    $ffOk = ($vf && count($vf) == 5 && is_nan($vf[0]) &&
             abs($vf[1]-1)<1e-9 && abs($vf[2]-1)<1e-9 &&
             abs($vf[3]-3)<1e-9 && abs($vf[4]-3)<1e-9);

    // bfill check
    $bfOk = ($vb && count($vb) == 5 &&
             abs($vb[0]-1)<1e-9 && abs($vb[1]-1)<1e-9 &&
             abs($vb[2]-3)<1e-9 && abs($vb[3]-3)<1e-9 &&
             is_nan($vb[4]));

    if ($ffOk) {
        ok("Test 9 - ffill on [NaN,1,NaN,3,NaN] -> [NaN,1,1,3,3]");
    } else {
        fail("Test 9 - ffill combo", "got ".json_encode($vf));
    }

    if ($bfOk) {
        ok("Test 10 - bfill on [NaN,1,NaN,3,NaN] -> [1,1,3,3,NaN]");
    } else {
        fail("Test 10 - bfill combo", "got ".json_encode($vb));
    }

    $ffi->pardox_free_manager($rFf2);
    $ffi->pardox_free_manager($rBf2);
} else {
    if (!$rFf2) fail("Test 9 - ffill combo returned null");
    if (!$rBf2) fail("Test 10 - bfill combo returned null");
}
$ffi->pardox_free_manager($mgrCombo);

// =============================================================================
// Section 4: Linear Interpolation
// =============================================================================
section("interpolate (Linear)");

// Series: [1, NaN, NaN, 4, NaN, NaN, NaN, 8]
// Interpolated: [1, 2, 3, 4, 5, 6, 7, 8]
$interpCsv = '_gap9_interp.csv';
$fp = fopen($interpCsv, 'w');
fwrite($fp, "val\n");
fwrite($fp, "1.0\n");
fwrite($fp, "NaN\n");
fwrite($fp, "NaN\n");
fwrite($fp, "4.0\n");
fwrite($fp, "NaN\n");
fwrite($fp, "NaN\n");
fwrite($fp, "NaN\n");
fwrite($fp, "8.0\n");
fclose($fp);

$mgrIp = load_csv($interpCsv, $ffi);
$rIp = $ffi->pardox_interpolate($mgrIp, "val");

// Test 11: returns non-null
if ($rIp) {
    ok("Test 11 - pardox_interpolate returns non-null");
} else {
    fail("Test 11 - interpolate returned null");
}

// Test 12: row count preserved
if ($rIp) {
    $rc = nrows($rIp, $ffi);
    if ($rc == 8) {
        ok("Test 12 - row count preserved (8)", "got $rc");
    } else {
        fail("Test 12 - row count", "expected 8, got $rc");
    }
}

// Test 13: interpolated values correct
if ($rIp) {
    $valsIp = get_f64_values($rIp, "val", $ffi);
    $expectedIp = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
    $match = true;
    if (count($valsIp) == 8) {
        for ($i = 0; $i < 8; $i++) {
            if (abs($valsIp[$i] - $expectedIp[$i]) > 1e-6) {
                $match = false;
                break;
            }
        }
    } else {
        $match = false;
    }
    if ($match) {
        ok("Test 13 - interpolated values correct", json_encode($valsIp));
    } else {
        fail("Test 13 - interpolated values", "expected ".json_encode($expectedIp).", got ".json_encode($valsIp));
    }
    $ffi->pardox_free_manager($rIp);
}

// Test 14: leading/trailing NaN stay NaN
$leadInterpCsv = '_gap9_lead_interp.csv';
$fp = fopen($leadInterpCsv, 'w');
fwrite($fp, "val\n");
fwrite($fp, "NaN\n");
fwrite($fp, "NaN\n");
fwrite($fp, "3.0\n");
fwrite($fp, "5.0\n");
fwrite($fp, "NaN\n");
fclose($fp);

$mgrLip = load_csv($leadInterpCsv, $ffi);
$rLip = $ffi->pardox_interpolate($mgrLip, "val");
if ($rLip) {
    $v = get_f64_values($rLip, "val", $ffi);
    if ($v && count($v) == 5 && is_nan($v[0]) && is_nan($v[1]) &&
        abs($v[2]-3)<1e-9 && abs($v[3]-5)<1e-9 && is_nan($v[4])) {
        ok("Test 14 - leading/trailing NaN stay after interpolate");
    } else {
        fail("Test 14 - edge NaN behavior", "got ".json_encode($v));
    }
    $ffi->pardox_free_manager($rLip);
} else {
    fail("Test 14 - interpolate edge NaN returned null");
}
$ffi->pardox_free_manager($mgrLip);

// Test 15: non-Float64 column returns null
$intCsv = '_gap9_int.csv';
$fp = fopen($intCsv, 'w');
fwrite($fp, "id,val\n");
fwrite($fp, "1,10\n");
fwrite($fp, "2,20\n");
fwrite($fp, "3,30\n");
fclose($fp);

$mgrInt = load_csv($intCsv, $ffi);
$rBad = $ffi->pardox_interpolate($mgrInt, "id");  // id is Int64
if (!$rBad || $rBad == 0) {
    ok("Test 15 - interpolate on Int64 column -> null (unsupported type)");
} else {
    fail("Test 15 - should have returned null");
    $ffi->pardox_free_manager($rBad);
}
$ffi->pardox_free_manager($mgrInt);

// =============================================================================
// Section 5: no-null input is identity
// =============================================================================
section("no-null data (identity check)");

$noNullCsv = '_gap9_nonull.csv';
$fp = fopen($noNullCsv, 'w');
fwrite($fp, "val\n");
fwrite($fp, "1.0\n");
fwrite($fp, "2.0\n");
fwrite($fp, "3.0\n");
fwrite($fp, "4.0\n");
fwrite($fp, "5.0\n");
fclose($fp);

$mgrNn = load_csv($noNullCsv, $ffi);
$rFfNn = $ffi->pardox_ffill($mgrNn, "val");
$rBfNn = $ffi->pardox_bfill($mgrNn, "val");
$rIpNn = $ffi->pardox_interpolate($mgrNn, "val");

$vFf = get_f64_values($rFfNn, "val", $ffi);
$vBf = get_f64_values($rBfNn, "val", $ffi);
$vIp = get_f64_values($rIpNn, "val", $ffi);
$expectedNn = [1.0, 2.0, 3.0, 4.0, 5.0];

if ($vFf == $expectedNn) {
    ok("Test 16 - ffill on non-null data is identity", json_encode($vFf));
} else {
    fail("Test 16 - ffill identity", "expected ".json_encode($expectedNn).", got ".json_encode($vFf));
}

if ($vBf == $expectedNn) {
    ok("Test 17 - bfill on non-null data is identity", json_encode($vBf));
} else {
    fail("Test 17 - bfill identity", "expected ".json_encode($expectedNn).", got ".json_encode($vBf));
}

if ($vIp == $expectedNn) {
    ok("Test 18 - interpolate on non-null data is identity", json_encode($vIp));
} else {
    fail("Test 18 - interp identity", "expected ".json_encode($expectedNn).", got ".json_encode($vIp));
}

if ($rFfNn) $ffi->pardox_free_manager($rFfNn);
if ($rBfNn) $ffi->pardox_free_manager($rBfNn);
if ($rIpNn) $ffi->pardox_free_manager($rIpNn);
$ffi->pardox_free_manager($mgrNn);

// =============================================================================
// Section 6: error paths
// =============================================================================
section("error paths");

$errCsv = '_gap9_err.csv';
$fp = fopen($errCsv, 'w');
fwrite($fp, "val\n");
fwrite($fp, "1.0\n");
fwrite($fp, "2.0\n");
fclose($fp);

$mgrErr = load_csv($errCsv, $ffi);

$r = $ffi->pardox_ffill($mgrErr, "__no_such__");
if (!$r || $r == 0) {
    ok("Test 19 - ffill invalid column -> null");
} else {
    fail("Test 19 - ffill should return null");
    $ffi->pardox_free_manager($r);
}

$r = $ffi->pardox_bfill($mgrErr, "__no_such__");
if (!$r || $r == 0) {
    ok("Test 20 - bfill invalid column -> null");
} else {
    fail("Test 20 - bfill should return null");
    $ffi->pardox_free_manager($r);
}

$r = $ffi->pardox_interpolate($mgrErr, "__no_such__");
if (!$r || $r == 0) {
    ok("Test 21 - interpolate invalid column -> null");
} else {
    fail("Test 21 - interpolate should return null");
    $ffi->pardox_free_manager($r);
}
$ffi->pardox_free_manager($mgrErr);

// Cleanup temp files
foreach (['_gap9_ffill.csv', '_gap9_lead.csv', '_gap9_bfill.csv', '_gap9_combo.csv',
          '_gap9_interp.csv', '_gap9_lead_interp.csv', '_gap9_int.csv',
          '_gap9_nonull.csv', '_gap9_err.csv'] as $f) {
    @unlink($here . '/' . $f);
}

// Summary
section("FINAL RESULT - Gap 9");
echo "\n";
echo "Results: $passed passed, $failed failed\n";

if ($failed == 0) {
    echo "  ALL TESTS PASSED - Gap 9 Time Series Fill VALIDATED\n";
} else {
    echo "  $failed TEST(S) FAILED - See output above for details\n";
}

exit($failed > 0 ? 1 : 0);

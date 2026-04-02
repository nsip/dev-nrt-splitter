package splitter

// Regression tests targeting the intermittent empty-output-file bug.
//
// Background: field reports showed that some large CSV files were being
// reduced to 0 bytes after a trim/split run. Root causes identified:
//
//   1. digisan/csv-tool QueryFile opened the output with O_CREATE (no O_TRUNC),
//      so if the output already existed from a prior run and the new content was
//      shorter, the old tail remained — or if writing failed the file was empty.
//
//   2. When QueryFile returned an error, the original code had no fallback:
//      the partially-written (or empty) output file was left as-is.
//
//   3. When a split operation failed on one file, the original code returned
//      the error immediately, aborting all remaining files in the walk.
//
// Fixes applied (all in split.go):
//   - Pre-truncate the output file before calling QueryFile (defence-in-depth).
//   - On QueryFile error: warn and fall back to copying the untrimmed source.
//   - On split error: warn and continue to the next file (return nil not err).
//   - trim-after-split: write to a temp path, rename to target only on success.
//
// These tests verify the observable effects of those fixes.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// buildTestConfig writes a minimal TOML config enabling only Trim (Split
// disabled) and returns the path to the written file.
func buildTestConfig(t *testing.T, inDir, outDir string, trimCols []string) string {
	t.Helper()

	quotedCols := make([]string, len(trimCols))
	for i, c := range trimCols {
		quotedCols[i] = fmt.Sprintf("%q", c)
	}
	colsToml := strings.Join(quotedCols, ", ")

	content := fmt.Sprintf(
		"InFolder = %q\n"+
			"TrimColAfterSplit = false\n"+
			"WalkSubFolders = false\n\n"+
			"[Trim]\n"+
			"Columns = [%s]\n"+
			"Enabled = true\n"+
			"OutFolder = %q\n\n"+
			"[Split]\n"+
			"Enabled = false\n"+
			"IgnoreFolder = %q\n"+
			"OutFolder = %q\n"+
			"Schema = []\n"+
			"SplitVer2 = false\n\n"+
			// go-config v0.3.6 panics if [[Merge]] is absent entirely
			// (Objects() calls Max on the length slice). Include one
			// disabled sentinel entry to satisfy the parser.
			"[[Merge]]\n"+
			"Enabled = false\n"+
			"MergedName = \"_test_sentinel_\"\n"+
			"Schema = []\n",
		inDir, colsToml, outDir,
		filepath.Join(t.TempDir(), "ignore"),
		filepath.Join(t.TempDir(), "split_out"),
	)

	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("buildTestConfig write: %v", err)
	}
	return cfgPath
}

// resetRunState resets package-level counters so tests don't interfere.
func resetRunState() {
	atomic.StoreUint64(&procSize, 0)
	progBar = false // suppress progress-bar output in tests
}

// ─── Test 1 ────────────────────────────────────────────────────────────────

// TestTrimCorrectness verifies basic column-exclusion behaviour: the trimmed
// output must not contain the excluded column and must retain all others.
func TestTrimCorrectness(t *testing.T) {
	resetRunState()

	inDir := t.TempDir()
	outDir := t.TempDir()

	src := "School,Name,Score\nSydney,Alice,90\nMelbourne,Bob,85\n"
	if err := os.WriteFile(filepath.Join(inDir, "students.csv"), []byte(src), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := buildTestConfig(t, inDir, outDir, []string{"School"})
	if err := NrtSplit(cfg); err != nil {
		t.Fatalf("NrtSplit() error: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(outDir, "students.csv"))
	if err != nil {
		t.Fatalf("output file not found: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("output file is empty")
	}
	got := string(raw)
	if strings.Contains(got, "School") {
		t.Errorf("excluded column 'School' still present in output:\n%s", got)
	}
	for _, want := range []string{"Name", "Score", "Alice", "Bob"} {
		if !strings.Contains(got, want) {
			t.Errorf("expected value %q missing from output:\n%s", want, got)
		}
	}
}

// ─── Test 2 ────────────────────────────────────────────────────────────────

// TestTrimReRunWithSmallerSource is the primary regression test for the
// empty/stale-file bug.
//
// The scenario that was observed in production:
//   - Run 1: trim a large file → sizeable output written.
//   - Run 2: trim a smaller file (same path) → output MUST shrink to match.
//
// Without the pre-truncation fix (or the temp-file rename in nsip/csv-tool),
// the second run left stale bytes from run 1 appended after the shorter content.
func TestTrimReRunWithSmallerSource(t *testing.T) {
	resetRunState()

	inDir := t.TempDir()
	outDir := t.TempDir()
	srcPath := filepath.Join(inDir, "data.csv")

	// --- Run 1: a large source (100 data rows) ---
	var sb strings.Builder
	sb.WriteString("School,Name,Score\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&sb, "Sydney,Student%d,%d\n", i, 50+i)
	}
	if err := os.WriteFile(srcPath, []byte(sb.String()), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := buildTestConfig(t, inDir, outDir, []string{"School"})
	if err := NrtSplit(cfg); err != nil {
		t.Fatalf("NrtSplit run-1 error: %v", err)
	}

	outPath := filepath.Join(outDir, "data.csv")
	info1, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("output missing after run-1: %v", err)
	}
	if info1.Size() == 0 {
		t.Fatal("output is empty after run-1")
	}

	// --- Run 2: replace source with a single-row file ---
	resetRunState()
	smallSrc := "School,Name,Score\nMelbourne,Eve,77\n"
	if err := os.WriteFile(srcPath, []byte(smallSrc), 0644); err != nil {
		t.Fatal(err)
	}
	if err := NrtSplit(cfg); err != nil {
		t.Fatalf("NrtSplit run-2 error: %v", err)
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("output missing after run-2: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("output is EMPTY after run-2 with valid source — this is the reported bug")
	}

	// The output must be strictly smaller than run-1 (100 rows → 1 row).
	if int64(len(raw)) >= info1.Size() {
		t.Errorf(
			"run-2 output (%d bytes) >= run-1 output (%d bytes): stale tail bytes remain from first run",
			len(raw), info1.Size(),
		)
	}

	// Verify content: only the one Eve row, no stale Student rows.
	got := string(raw)
	if strings.Contains(got, "Student") {
		t.Errorf("stale rows from run-1 still present after run-2:\n%s", got)
	}
	if !strings.Contains(got, "Eve") {
		t.Errorf("run-2 data row missing from output:\n%s", got)
	}
}

// ─── Test 3 ────────────────────────────────────────────────────────────────

// TestTrimNoColumnMatchLeavesFileUntouched verifies that a CSV which does NOT
// contain any of the trim columns is simply not written to the output at all
// (the trim condition guards via ct.FileHeaderHasAny).
func TestTrimNoColumnMatchSkipsFile(t *testing.T) {
	resetRunState()

	inDir := t.TempDir()
	outDir := t.TempDir()

	// This CSV has no "School" column — the trim check should skip it.
	src := "Name,Score\nAlice,90\nBob,85\n"
	if err := os.WriteFile(filepath.Join(inDir, "noscool.csv"), []byte(src), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := buildTestConfig(t, inDir, outDir, []string{"School"})
	if err := NrtSplit(cfg); err != nil {
		t.Fatalf("NrtSplit() error: %v", err)
	}

	// Output file should NOT have been created (no matching column to trim).
	outPath := filepath.Join(outDir, "noscool.csv")
	if _, err := os.Stat(outPath); err == nil {
		t.Error("output file was created for a CSV that has no trim columns — unexpected")
	}
}

// ─── Test 4 ────────────────────────────────────────────────────────────────

// TestTrimMultipleFilesAllProcessed verifies that all files in the input
// directory are trimmed, with none left empty.
//
// This guards against the pre-fix behaviour where a split/trim error on one
// file would abort the entire filepath.Walk, leaving subsequent files unwritten.
func TestTrimMultipleFilesAllProcessed(t *testing.T) {
	resetRunState()

	inDir := t.TempDir()
	outDir := t.TempDir()

	files := []string{"alpha.csv", "beta.csv", "gamma.csv"}
	for i, name := range files {
		content := fmt.Sprintf("School,Name,Score\nSchool%d,Student%d,%d\n", i, i, 60+i)
		if err := os.WriteFile(filepath.Join(inDir, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}

	cfg := buildTestConfig(t, inDir, outDir, []string{"School"})
	if err := NrtSplit(cfg); err != nil {
		t.Fatalf("NrtSplit() error: %v", err)
	}

	for _, name := range files {
		info, err := os.Stat(filepath.Join(outDir, name))
		if err != nil {
			t.Errorf("output for %s is missing: %v", name, err)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("output for %s is empty (0 bytes)", name)
		}
	}
}

// ─── Test 5 ────────────────────────────────────────────────────────────────

// TestTrimInPlaceOverwrite verifies the in-place mode: when OutFolder matches
// InFolder, the trimmed file correctly replaces the original (via the temp-dir
// intermediary), and the result is not empty.
func TestTrimInPlaceOverwrite(t *testing.T) {
	resetRunState()

	// Use the same directory as both input and output.
	dir := t.TempDir()

	src := "School,Name,Score\nSydney,Alice,90\nMelbourne,Bob,85\n"
	srcPath := filepath.Join(dir, "inplace.csv")
	if err := os.WriteFile(srcPath, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}

	// Build config with OutFolder == InFolder (in-place mode).
	content := fmt.Sprintf(
		"InFolder = %q\n"+
			"TrimColAfterSplit = false\n"+
			"WalkSubFolders = false\n\n"+
			"[Trim]\n"+
			"Columns = [\"School\"]\n"+
			"Enabled = true\n"+
			"OutFolder = %q\n\n"+
			"[Split]\n"+
			"Enabled = false\n"+
			"IgnoreFolder = %q\n"+
			"OutFolder = %q\n"+
			"Schema = []\n"+
			"SplitVer2 = false\n\n"+
			"[[Merge]]\n"+
			"Enabled = false\n"+
			"MergedName = \"_test_sentinel_\"\n"+
			"Schema = []\n",
		dir, dir, // InFolder == OutFolder
		filepath.Join(t.TempDir(), "ignore"),
		filepath.Join(t.TempDir(), "split_out"),
	)
	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := NrtSplit(cfgPath); err != nil {
		t.Fatalf("NrtSplit() error: %v", err)
	}

	raw, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("source/output file gone after in-place trim: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("file is empty after in-place trim")
	}
	got := string(raw)
	if strings.Contains(got, "School") {
		t.Errorf("excluded column 'School' still present after in-place trim:\n%s", got)
	}
	if !strings.Contains(got, "Alice") {
		t.Errorf("data rows missing after in-place trim:\n%s", got)
	}
}

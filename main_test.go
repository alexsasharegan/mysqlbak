package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestConfigVsFlagPrecedence(t *testing.T) {
	tmpDir := t.TempDir()
	confPath := filepath.Join(tmpDir, "config.toml")
	bakDir := filepath.Join(tmpDir, "flag_bak")

	if err := os.MkdirAll(bakDir, 0755); err != nil {
		t.Fatalf("failed to create temp bak dir: %v", err)
	}

	confContent := []byte(`BaseDir = "/custom/config/dir"` + "\n" + `Databases = ["db1"]`)
	if err := os.WriteFile(confPath, confContent, 0644); err != nil {
		t.Fatalf("failed to write mock config: %v", err)
	}

	// 1. Config file sets BaseDir, no -bak flag passed -> Uses config BaseDir
	p1 := &Program{BackupPath: defBak}
	if err := resolveConf(p1, confPath); err != nil {
		t.Fatalf("resolveConf failed: %v", err)
	}
	if p1.BackupPath != "/custom/config/dir" {
		t.Errorf("expected BackupPath from config /custom/config/dir, got %q", p1.BackupPath)
	}

	// 2. -bak flag explicitly passed -> Overrides config BaseDir
	var outBuf, errBuf bytes.Buffer
	args := []string{"-c", confPath, "-bak", bakDir, "test_db"}

	_ = run(args, &outBuf, &errBuf)

	// Verify database subfolder was created in explicit -bak path (proving flag won over config)
	if _, err := os.Stat(filepath.Join(bakDir, "test_db")); os.IsNotExist(err) {
		t.Errorf("expected database dir in explicit -bak path %q, but it was not created", bakDir)
	}
}

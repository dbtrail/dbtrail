//go:build unix

package icebergexport

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLockWarehouse_secondWriterRefused(t *testing.T) {
	p := filepath.Join(t.TempDir(), lockFileName)
	release, err := lockWarehouse(p)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lockWarehouse(p)
	if err == nil || !strings.Contains(err.Error(), "another Iceberg export is running") {
		release()
		t.Fatalf("second lock: err = %v, want the single-writer refusal", err)
	}
	release()
	release2, err := lockWarehouse(p)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	release2()
}

// TestOpenWarehouse_takesTheLock: the opener, not only lockWarehouse, holds
// the single-writer lock for as long as the catalog is in use.
func TestOpenWarehouse_takesTheLock(t *testing.T) {
	dir := t.TempDir()
	_, release, err := openWarehouse(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	_, release2, err := openWarehouse(t.Context(), dir)
	if err == nil {
		release2()
		release()
		t.Fatal("second openWarehouse on the same directory succeeded; want the single-writer refusal")
	}
	if !strings.Contains(err.Error(), "another Iceberg export is running") {
		release()
		t.Fatalf("second openWarehouse: err = %v, want the single-writer refusal", err)
	}
	release()
}

package postgres

import (
	"testing"
	"testing/fstest"
)

func TestLoadMigrationsOrdersByVersion(t *testing.T) {
	fsys := fstest.MapFS{
		"0003_add_index.sql":   {Data: []byte("SELECT 3")},
		"0001_create_jobs.sql": {Data: []byte("SELECT 1")},
		"0002_add_column.sql":  {Data: []byte("SELECT 2")},
		"README.md":            {Data: []byte("not a migration")},
	}

	migrations, err := loadMigrations(fsys)
	if err != nil {
		t.Fatalf("loadMigrations() error = %v", err)
	}
	if len(migrations) != 3 {
		t.Fatalf("len(migrations) = %d, want 3", len(migrations))
	}
	for i, want := range []int64{1, 2, 3} {
		if migrations[i].version != want {
			t.Errorf("migrations[%d].version = %d, want %d", i, migrations[i].version, want)
		}
	}
}

func TestLoadMigrationsRejectsBadNames(t *testing.T) {
	tests := []struct {
		name string
		fsys fstest.MapFS
	}{
		{
			name: "missing description",
			fsys: fstest.MapFS{"0001.sql": {Data: []byte("SELECT 1")}},
		},
		{
			name: "non-numeric version",
			fsys: fstest.MapFS{"abc_create.sql": {Data: []byte("SELECT 1")}},
		},
		{
			name: "duplicate version",
			fsys: fstest.MapFS{
				"0001_create.sql": {Data: []byte("SELECT 1")},
				"0001_other.sql":  {Data: []byte("SELECT 2")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := loadMigrations(tt.fsys); err == nil {
				t.Fatal("loadMigrations() error = nil, want error")
			}
		})
	}
}

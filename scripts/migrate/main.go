package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	migrationsTable = "schema_migrations"
	migrationsDir   = "scripts/migrations"
)

type Migration struct {
	Version  int
	Name     string
	UpPath   string
	DownPath string
}

func main() {
	// Parse flags
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL")
	migrationsPath := flag.String("migrations-path", migrationsDir, "Path to migrations directory")
	flag.Parse()

	if *databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable or --database-url flag is required")
	}

	if len(flag.Args()) < 1 {
		printUsage()
		os.Exit(1)
	}

	command := flag.Args()[0]

	// Connect to database
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Ensure migrations table exists
	if err := ensureMigrationsTable(ctx, pool); err != nil {
		log.Fatalf("Failed to ensure migrations table: %v", err)
	}

	// Load migrations from filesystem
	migrations, err := loadMigrations(*migrationsPath)
	if err != nil {
		log.Fatalf("Failed to load migrations: %v", err)
	}

	switch command {
	case "up":
		if err := runMigrationsUp(ctx, pool, migrations); err != nil {
			log.Fatalf("Migration up failed: %v", err)
		}
	case "down":
		steps := 1
		if len(flag.Args()) > 1 {
			steps, err = strconv.Atoi(flag.Args()[1])
			if err != nil {
				log.Fatalf("Invalid number of steps: %v", err)
			}
		}
		if err := runMigrationsDown(ctx, pool, migrations, steps); err != nil {
			log.Fatalf("Migration down failed: %v", err)
		}
	case "status":
		if err := showStatus(ctx, pool, migrations); err != nil {
			log.Fatalf("Failed to show status: %v", err)
		}
	case "version":
		version, err := getCurrentVersion(ctx, pool)
		if err != nil {
			log.Fatalf("Failed to get version: %v", err)
		}
		fmt.Printf("Current migration version: %d\n", version)
	case "force":
		if len(flag.Args()) < 2 {
			log.Fatal("Usage: migrate force <version>")
		}
		version, err := strconv.Atoi(flag.Args()[1])
		if err != nil {
			log.Fatalf("Invalid version: %v", err)
		}
		if err := forceVersion(ctx, pool, version); err != nil {
			log.Fatalf("Failed to force version: %v", err)
		}
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: migrate [options] <command> [args]

Commands:
  up             Run all pending migrations
  down [n]       Rollback n migrations (default: 1)
  status         Show migration status
  version        Show current migration version
  force <n>      Force set migration version (removes dirty state)

Options:
  --database-url    PostgreSQL connection URL (or set DATABASE_URL env var)
  --migrations-path Path to migrations directory (default: scripts/migrations)

Examples:
  migrate up
  migrate down
  migrate down 3
  migrate status
  migrate version
  migrate force 5`)
}

func ensureMigrationsTable(ctx context.Context, pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version INTEGER PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			dirty BOOLEAN NOT NULL DEFAULT FALSE
		)
	`, migrationsTable)

	_, err := pool.Exec(ctx, query)
	return err
}

func loadMigrations(dir string) ([]Migration, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	migrationMap := make(map[int]*Migration)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}

		// Parse migration filename: 001_initial.up.sql or 001_initial.down.sql
		parts := strings.Split(name, "_")
		if len(parts) < 2 {
			continue
		}

		version, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}

		isUp := strings.HasSuffix(name, ".up.sql")
		isDown := strings.HasSuffix(name, ".down.sql")

		if !isUp && !isDown {
			continue
		}

		if migrationMap[version] == nil {
			// Extract migration name (remove version prefix and .up.sql/.down.sql suffix)
			migrationName := strings.TrimPrefix(name, parts[0]+"_")
			migrationName = strings.TrimSuffix(migrationName, ".up.sql")
			migrationName = strings.TrimSuffix(migrationName, ".down.sql")

			migrationMap[version] = &Migration{
				Version: version,
				Name:    migrationName,
			}
		}

		fullPath := filepath.Join(dir, name)
		if isUp {
			migrationMap[version].UpPath = fullPath
		} else {
			migrationMap[version].DownPath = fullPath
		}
	}

	// Convert map to sorted slice
	var migrations []Migration
	for _, m := range migrationMap {
		if m.UpPath != "" { // Only include migrations with up files
			migrations = append(migrations, *m)
		}
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

func getCurrentVersion(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var version int
	query := fmt.Sprintf(`SELECT COALESCE(MAX(version), 0) FROM %s WHERE NOT dirty`, migrationsTable)
	err := pool.QueryRow(ctx, query).Scan(&version)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func getAppliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[int]bool, error) {
	query := fmt.Sprintf(`SELECT version FROM %s`, migrationsTable)
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}

	return applied, rows.Err()
}

func runMigrationsUp(ctx context.Context, pool *pgxpool.Pool, migrations []Migration) error {
	applied, err := getAppliedVersions(ctx, pool)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}

		fmt.Printf("Applying migration %d: %s...\n", m.Version, m.Name)

		// Read migration file
		content, err := os.ReadFile(m.UpPath)
		if err != nil {
			return fmt.Errorf("failed to read migration %d: %w", m.Version, err)
		}

		// Start transaction
		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		// Mark as dirty before running
		markDirtyQuery := fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES ($1, TRUE)`, migrationsTable)
		if _, err := tx.Exec(ctx, markDirtyQuery, m.Version); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to mark migration %d as dirty: %w", m.Version, err)
		}

		// Execute migration
		if _, err := tx.Exec(ctx, string(content)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to execute migration %d: %w", m.Version, err)
		}

		// Mark as clean
		markCleanQuery := fmt.Sprintf(`UPDATE %s SET dirty = FALSE, applied_at = NOW() WHERE version = $1`, migrationsTable)
		if _, err := tx.Exec(ctx, markCleanQuery, m.Version); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to mark migration %d as clean: %w", m.Version, err)
		}

		// Commit transaction
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", m.Version, err)
		}

		fmt.Printf("  ✓ Applied migration %d\n", m.Version)
	}

	fmt.Println("All migrations applied successfully!")
	return nil
}

func runMigrationsDown(ctx context.Context, pool *pgxpool.Pool, migrations []Migration, steps int) error {
	applied, err := getAppliedVersions(ctx, pool)
	if err != nil {
		return err
	}

	// Reverse the migrations slice for rolling back
	reversed := make([]Migration, len(migrations))
	copy(reversed, migrations)
	sort.Slice(reversed, func(i, j int) bool {
		return reversed[i].Version > reversed[j].Version
	})

	rolledBack := 0
	for _, m := range reversed {
		if !applied[m.Version] {
			continue
		}

		if rolledBack >= steps {
			break
		}

		if m.DownPath == "" {
			return fmt.Errorf("migration %d has no down file", m.Version)
		}

		fmt.Printf("Rolling back migration %d: %s...\n", m.Version, m.Name)

		// Read migration file
		content, err := os.ReadFile(m.DownPath)
		if err != nil {
			return fmt.Errorf("failed to read migration %d down file: %w", m.Version, err)
		}

		// Start transaction
		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		// Execute rollback
		if _, err := tx.Exec(ctx, string(content)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to execute rollback for migration %d: %w", m.Version, err)
		}

		// Remove from schema_migrations
		deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE version = $1`, migrationsTable)
		if _, err := tx.Exec(ctx, deleteQuery, m.Version); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to remove migration %d from schema_migrations: %w", m.Version, err)
		}

		// Commit transaction
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit rollback for migration %d: %w", m.Version, err)
		}

		fmt.Printf("  ✓ Rolled back migration %d\n", m.Version)
		rolledBack++
	}

	if rolledBack == 0 {
		fmt.Println("No migrations to roll back.")
	} else {
		fmt.Printf("Rolled back %d migration(s) successfully!\n", rolledBack)
	}

	return nil
}

func showStatus(ctx context.Context, pool *pgxpool.Pool, migrations []Migration) error {
	// Get applied migrations with their applied_at time
	query := fmt.Sprintf(`SELECT version, applied_at, dirty FROM %s ORDER BY version`, migrationsTable)
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	appliedMap := make(map[int]struct {
		AppliedAt time.Time
		Dirty     bool
	})

	for rows.Next() {
		var version int
		var appliedAt time.Time
		var dirty bool
		if err := rows.Scan(&version, &appliedAt, &dirty); err != nil {
			return err
		}
		appliedMap[version] = struct {
			AppliedAt time.Time
			Dirty     bool
		}{appliedAt, dirty}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	fmt.Println("\nMigration Status:")
	fmt.Println("─────────────────────────────────────────────────────────────────")
	fmt.Printf("%-8s %-40s %-10s %s\n", "VERSION", "NAME", "STATUS", "APPLIED AT")
	fmt.Println("─────────────────────────────────────────────────────────────────")

	for _, m := range migrations {
		status := "pending"
		appliedAt := ""

		if info, ok := appliedMap[m.Version]; ok {
			if info.Dirty {
				status = "dirty"
			} else {
				status = "applied"
			}
			appliedAt = info.AppliedAt.Format("2006-01-02 15:04:05")
		}

		statusColor := ""
		switch status {
		case "applied":
			statusColor = "\033[32m" // Green
		case "pending":
			statusColor = "\033[33m" // Yellow
		case "dirty":
			statusColor = "\033[31m" // Red
		}
		resetColor := "\033[0m"

		fmt.Printf("%-8d %-40s %s%-10s%s %s\n", m.Version, m.Name, statusColor, status, resetColor, appliedAt)
	}

	fmt.Println("─────────────────────────────────────────────────────────────────")

	currentVersion, _ := getCurrentVersion(ctx, pool)
	fmt.Printf("\nCurrent Version: %d\n", currentVersion)
	fmt.Printf("Total Migrations: %d\n", len(migrations))

	appliedCount := 0
	for _, m := range migrations {
		if _, ok := appliedMap[m.Version]; ok {
			appliedCount++
		}
	}
	fmt.Printf("Applied: %d, Pending: %d\n\n", appliedCount, len(migrations)-appliedCount)

	return nil
}

func forceVersion(ctx context.Context, pool *pgxpool.Pool, version int) error {
	// Delete all migration records
	deleteQuery := fmt.Sprintf(`DELETE FROM %s`, migrationsTable)
	if _, err := pool.Exec(ctx, deleteQuery); err != nil {
		return fmt.Errorf("failed to clear migrations table: %w", err)
	}

	// Insert records for all versions up to the forced version
	if version > 0 {
		for v := 1; v <= version; v++ {
			insertQuery := fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES ($1, FALSE)`, migrationsTable)
			if _, err := pool.Exec(ctx, insertQuery, v); err != nil {
				return fmt.Errorf("failed to insert version %d: %w", v, err)
			}
		}
	}

	fmt.Printf("Forced migration version to %d\n", version)
	return nil
}

package indexer

import (
	"fmt"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// EnsureDatabase creates the index database if it does not already exist. cfg is
// a parsed DSN config pointing at the MySQL server (its DBName is ignored — the
// connection is opened without a database so CREATE DATABASE can run); dbName is
// created with utf8mb4/utf8mb4_unicode_ci.
//
// The optional log callback receives a human-readable "ready" line. Pass nil for
// silent provisioning (e.g. the monitor supervisor); the CLI passes a closure
// that prints to stdout.
func EnsureDatabase(cfg *mysql.Config, dbName string, log func(string)) error {
	if log == nil {
		log = func(string) {}
	}

	serverCfg := *cfg
	serverCfg.DBName = ""

	db, err := config.OpenMySQL(&serverCfg)
	if err != nil {
		return fmt.Errorf("failed to open server connection: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to connect to MySQL server: %w", err)
	}

	q := fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
		dbName,
	)
	if _, err := db.Exec(q); err != nil {
		return fmt.Errorf("failed to create database %q: %w", dbName, err)
	}

	log(fmt.Sprintf("Database %q ready.", dbName))
	return nil
}

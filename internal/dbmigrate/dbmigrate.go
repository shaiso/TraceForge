// Package dbmigrate накатывает встроенные (go:embed) миграции golang-migrate.
// Единая точка для всех БД проекта — сейчас Postgres, позже ClickHouse.
package dbmigrate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"traceforge/migrations"
)

// Postgres накатывает все up-миграции на БД по dsn (формат postgres://user:pass@host:port/db).
// Идемпотентно: повторный запуск без новых миграций — no-op.
func Postgres(dsn string) error {
	src, err := iofs.New(migrations.Postgres, "postgres")
	if err != nil {
		return fmt.Errorf("dbmigrate: источник миграций: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, toPgxURL(dsn))
	if err != nil {
		return fmt.Errorf("dbmigrate: инициализация: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("dbmigrate: up: %w", err)
	}
	return nil
}

// toPgxURL меняет схему на pgx5:// — её ждёт драйвер БД golang-migrate.
func toPgxURL(dsn string) string {
	for _, p := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(dsn, p) {
			return "pgx5://" + strings.TrimPrefix(dsn, p)
		}
	}
	return dsn
}

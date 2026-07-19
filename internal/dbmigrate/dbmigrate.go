// Package dbmigrate накатывает встроенные (go:embed) миграции golang-migrate.
// Единая точка для всех БД проекта — сейчас Postgres, позже ClickHouse.
package dbmigrate

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
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

// ClickHouse накатывает все up-миграции на CH по dsn (натуральный формат
// clickhouse://user:pass@host:port/db — тот же, что у clickhouse-go). Идемпотентно.
func ClickHouse(dsn string) error {
	src, err := iofs.New(migrations.ClickHouse, "clickhouse")
	if err != nil {
		return fmt.Errorf("dbmigrate: источник ch-миграций: %w", err)
	}
	mURL, err := toMigrateCHURL(dsn)
	if err != nil {
		return fmt.Errorf("dbmigrate: ch dsn: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, mURL)
	if err != nil {
		return fmt.Errorf("dbmigrate: ch инициализация: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("dbmigrate: ch up: %w", err)
	}
	return nil
}

// toMigrateCHURL переводит натуральный DSN (clickhouse://user:pass@host/db) в форму,
// которую ждёт clickhouse-драйвер golang-migrate: логин/пароль/БД — в query-параметрах.
func toMigrateCHURL(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := url.Values{}
	if u.User != nil {
		q.Set("username", u.User.Username())
		if p, ok := u.User.Password(); ok {
			q.Set("password", p)
		}
	}
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		q.Set("database", db)
	}
	q.Set("x-multi-statement", "true")
	return "clickhouse://" + u.Host + "?" + q.Encode(), nil
}

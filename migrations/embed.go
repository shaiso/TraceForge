// Package migrations встраивает SQL-миграции в бинарь, чтобы их не таскать
// файлами рядом с деплоем. Каждая БД — свой подкаталог (postgres/, позже
// clickhouse/), накатываются через internal/dbmigrate.
package migrations

import "embed"

//go:embed postgres/*.sql
var Postgres embed.FS

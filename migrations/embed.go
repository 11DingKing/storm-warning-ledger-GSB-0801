// Package migrations 暴露嵌入的 SQL 迁移文件，供 store 的 migrator 使用。
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

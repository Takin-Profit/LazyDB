// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { createRequire } from "node:module"
import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"
import {
	isQueryKeyDef,
	validateQueryKeys,
	type LazyDbColumnType,
	type LazyDbValue,
	type NodeSqliteValue,
	type QueryKeys,
} from "./types.js"
import { isValidationErrors } from "./utils.js"

import type stringifyLib from "fast-safe-stringify"
const stringify: typeof stringifyLib.default = createRequire(import.meta.url)(
	"fast-safe-stringify"
).default
export function buildCreateTableSQL<T>(
	name: string,
	queryKeys?: QueryKeys<T>
): string {
	// biome-ignore lint/style/noUnusedTemplateLiteral: <explanation>
	const columns = [`_id INTEGER PRIMARY KEY AUTOINCREMENT`]

	if (queryKeys) {
		// Validate query keys schema
		const validationResult = validateQueryKeys({ queryKeys })
		if (isValidationErrors(validationResult)) {
			console.log(`invalid query keys schema: ${stringify(validationResult)}`)
			throw new NodeSqliteError(
				"ERR_SQLITE_SCHEMA",
				SqlitePrimaryResultCode.SQLITE_SCHEMA,
				"Invalid query keys schema",
				`Schema validation failed: ${validationResult.map((e) => e.message).join(", ")}`,
				undefined
			)
		}

		// Add columns for queryable fields
		for (const [field, def] of Object.entries(queryKeys)) {
			console.log(`field: ${field}, def: ${def}`)
			const constraints: string[] = []

			if (isQueryKeyDef(def)) {
				if (!def?.nullable) {
					constraints.push("NOT NULL")
				}
				if (def?.default !== undefined) {
					constraints.push(
						`DEFAULT ${
							typeof def.default === "string" ? `'${def.default}'` : def.default
						}`
					)
				}

				// If index.unique is true, add UNIQUE constraint
				if (def?.unique) {
					constraints.push("UNIQUE")
				}
				columns.push(
					`${field} ${def.type}${constraints?.length ? ` ${constraints.join(" ")}` : ""}`
				)
			}
		}
	}

	// Add data BLOB column for non-queryable fields
	columns.push("data BLOB")

	return `CREATE TABLE IF NOT EXISTS ${name} (${columns.join(", ")})`
}

export const createIndexes = <T extends { [key: string]: unknown }>(
	name: string,
	queryKeys: QueryKeys<T>
) =>
	Object.entries(queryKeys).map(([field, def]) => {
		const indexName = `idx_${name}_${field}`
		const indexType = def?.unique ? " UNIQUE" : ""
		return `CREATE${indexType} INDEX IF NOT EXISTS ${indexName} ON ${name}(${field})`
	})

export function toSqliteValue(
	value: LazyDbValue,
	columnType: LazyDbColumnType
): NodeSqliteValue {
	switch (columnType) {
		case "TEXT": {
			if (typeof value !== "string") {
				throw new TypeError(`Invalid value for TEXT: ${value}`)
			}
			return value
		}

		case "INTEGER": {
			if (typeof value === "number") {
				if (!Number.isInteger(value)) {
					throw new TypeError(
						`Invalid value for INTEGER: ${value} (must be an integer)`
					)
				}
				return value
			}
			if (typeof value === "bigint") {
				return value
			}
			if (typeof value === "boolean") {
				return value ? 1 : 0
			}
			throw new TypeError(`Invalid value for INTEGER: ${value}`)
		}

		case "REAL": {
			if (typeof value !== "number") {
				throw new TypeError(`Invalid value for REAL: ${value}`)
			}
			return value
		}

		case "BOOLEAN": {
			if (typeof value !== "boolean") {
				throw new TypeError(`Invalid value for BOOLEAN: ${value}`)
			}
			return value ? 1 : 0
		}

		default:
			throw new TypeError(`Unsupported SQLite column type: ${columnType}`)
	}
}

// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { SupportedValueType } from "node:sqlite"
import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"
import {
	type EntityType,
	isQueryKeyDef,
	validateQueryKeys,
	type LazyDbColumnType,
	type LazyDbValue,
	type NodeSqliteValue,
	type QueryKeys,
} from "./types.js"
import { isValidationErrors } from "./utils.js"

export function buildCreateTableSQL<T extends EntityType>(
	name: string,
	queryKeys?: QueryKeys<T>,
	timestamps = false
): string {
	// biome-ignore lint/style/noUnusedTemplateLiteral: <explanation>
	const columns = [`_id INTEGER PRIMARY KEY AUTOINCREMENT`]

	if (queryKeys) {
		// Validate query keys schema
		const validationResult = validateQueryKeys({ queryKeys })
		if (isValidationErrors(validationResult)) {
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

	if (timestamps) {
		// Add createdAt and updatedAt columns
		columns.push("createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")
		columns.push("updatedAt TEXT")
	}

	// Add data BLOB column for non-queryable fields
	columns.push("__lazy_data BLOB")

	return `CREATE TABLE IF NOT EXISTS ${name} (${columns.join(", ")})`
}

export const createIndexes = <T extends EntityType>(
	name: string,
	queryKeys: QueryKeys<T>
) =>
	Object.entries(queryKeys).map(([field, def]) => {
		if (isQueryKeyDef(def)) {
			const indexName = `idx_${name}_${field}`
			const indexType = def?.unique ? " UNIQUE" : ""
			return `CREATE${indexType} INDEX IF NOT EXISTS ${indexName} ON ${name}(${field})`
		}
		return ""
	})

export function toSqliteValue(
	value: LazyDbValue,
	columnType: LazyDbColumnType
): NodeSqliteValue {
	if (value === null) {
		return null
	}

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

interface InsertQueryResult {
	sql: string
	values: SupportedValueType[]
}

export function buildInsertQuery<T extends Record<string, unknown> | object>(
	tableName: string,
	entity: Omit<T, "_id" | "createdAt" | "updatedAt">,
	queryKeys?: QueryKeys<T>
): InsertQueryResult {
	const columns: string[] = []
	const values: SupportedValueType[] = []
	const placeholders: string[] = []

	// Add queryable fields if they exist
	if (queryKeys) {
		for (const [field, def] of Object.entries(queryKeys)) {
			const value =
				entity[field as keyof Omit<T, "_id" | "createdAt" | "updatedAt">]
			if (field in entity && isQueryKeyDef(def) && value !== undefined) {
				columns.push(field)
				values.push(toSqliteValue(value as unknown as LazyDbValue, def.type))
				placeholders.push("?")
			}
		}
	}

	// Add the serialized data column
	columns.push("__lazy_data")
	placeholders.push("?")

	// Build INSERT query
	const sql = `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders.join(", ")})`

	return { sql, values }
}

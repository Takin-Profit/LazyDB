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
	type QueryKeysSchema,
	type QueryKeys,
} from "./types.js"
import {
	createNestedColumnDefinitions,
	createNestedIndexDefinitions,
	extractQueryableValues,
} from "./paths.js"
import { isValidationErrs } from "./validate.js"

export function buildCreateTableSQL<T extends EntityType>(
	name: string,
	queryKeys?: QueryKeysSchema<T>,
	timestamps = false
): string {
	const columns = ["_id INTEGER PRIMARY KEY AUTOINCREMENT"]

	if (queryKeys) {
		// Validate query keys schema
		const validationResult = validateQueryKeys(queryKeys)

		if (isValidationErrs(validationResult)) {
			const errorMessages = validationResult
				.map((e) => `${e.path ? `${e.path}: ` : ""}${e.message}`)
				.join(", ")

			throw new NodeSqliteError(
				"ERR_SQLITE_SCHEMA",
				SqlitePrimaryResultCode.SQLITE_SCHEMA,
				"Invalid query keys schema",
				`Schema validation failed: ${errorMessages}`,
				undefined
			)
		}

		const ignorableFields = ["_id", "createdAt", "__lazy_data", "updatedAt"]

		// Add columns for regular queryable fields
		for (const [field, def] of Object.entries(queryKeys)) {
			if (ignorableFields.includes(field)) {
				continue
			}
			if (isQueryKeyDef(def) && !field.includes(".")) {
				const constraints: string[] = []

				if (!def?.nullable) {
					constraints.push("NOT NULL")
				}
				if (def?.default !== undefined) {
					constraints.push(
						`DEFAULT ${typeof def.default === "string" ? `'${def.default}'` : def.default}`
					)
				}
				if (def?.unique) {
					constraints.push("UNIQUE")
				}
				columns.push(
					`${field} ${def.type}${constraints.length ? ` ${constraints.join(" ")}` : ""}`
				)
			}
		}

		// Add nested columns outside the main loop
		const nestedColumns = createNestedColumnDefinitions(queryKeys)
		columns.push(...nestedColumns)
	}

	if (timestamps) {
		columns.push("createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")
		columns.push("updatedAt TEXT")
	}

	columns.push("__lazy_data BLOB")

	return `CREATE TABLE IF NOT EXISTS ${name} (${columns.join(", ")})`
}

export function createIndexes<T extends EntityType>(
	name: string,
	queryKeys: QueryKeysSchema<T>
): string[] {
	const regularIndexes = Object.entries(queryKeys)
		.map(([field, def]) => {
			if (!field.includes(".") && field !== "_id" && isQueryKeyDef(def)) {
				const indexName = `idx_${name}_${field}`
				const indexType = def?.unique ? " UNIQUE" : ""
				return `CREATE${indexType} INDEX IF NOT EXISTS ${indexName} ON ${name}(${field})`
			}
			return ""
		})
		.filter(Boolean)

	const nestedIndexes = createNestedIndexDefinitions(name, queryKeys)

	return [...regularIndexes, ...nestedIndexes]
}

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

/**
 * Builds the RETURNING clause for SQL queries.
 * @param timestamps - Whether timestamps (createdAt, updatedAt) are enabled.
 * @returns The RETURNING clause as a string.
 */
export function buildReturningClause(timestamps: boolean): string {
	const fields = ["_id", "__lazy_data"]
	if (timestamps) {
		fields.push("createdAt", "updatedAt")
	}
	return `RETURNING ${fields.join(", ")}`
}

interface InsertQueryResult {
	sql: string
	values: SupportedValueType[]
}

export function buildInsertQuery<T extends Record<string, unknown> | object>(
	tableName: string,
	entity: T,
	queryKeys?: QueryKeys<T>,
	timestamps = false
): InsertQueryResult {
	const columns: string[] = []
	const values: SupportedValueType[] = []
	const placeholders: string[] = []

	const ignorableFields = ["_id", "createdAt", "updatedAt"]
	if (queryKeys) {
		// Add queryable fields if they exist
		for (const [field, def] of Object.entries(queryKeys)) {
			if (ignorableFields.includes(field)) {
				continue
			}
			if (!field.includes(".") && isQueryKeyDef(def)) {
				const value =
					entity[field as keyof Omit<T, "_id" | "createdAt" | "updatedAt">]
				if (field in entity) {
					columns.push(field)
					values.push(toSqliteValue(value as unknown as LazyDbValue, def.type))
					placeholders.push("?")
				}
			}
		}

		// Handle nested fields
		const nestedValues = extractQueryableValues(entity, queryKeys)
		for (const [columnName, value] of Object.entries(nestedValues)) {
			columns.push(columnName)
			values.push(value as SupportedValueType)
			placeholders.push("?")
		}
	}

	// Add the serialized data column
	columns.push("__lazy_data")
	placeholders.push("?")

	const sql = `INSERT INTO ${tableName} (${columns.join(
		", "
	)}) VALUES (${placeholders.join(", ")}) ${buildReturningClause(timestamps)}`

	return { sql, values }
}

interface InsertManyQueryResult {
	sql: string
	values: SupportedValueType[][]
}

/**
 * Builds an SQL query for inserting multiple entities.
 *
 * @param tableName The name of the table to insert into
 * @param entities Array of entities to insert
 * @param queryKeys Optional query keys configuration
 * @param timestamps Whether to include timestamp fields
 * @returns Object containing the SQL query and array of value arrays
 */
export function buildInsertManyQuery<T extends EntityType>(
	tableName: string,
	entities: T[],
	queryKeys?: QueryKeys<T>,
	timestamps = false
): InsertManyQueryResult {
	if (!entities.length) {
		return { sql: "", values: [] }
	}

	const columns: string[] = []
	const values: SupportedValueType[][] = []
	const placeholders: string[] = []
	const ignorableFields = ["_id", "createdAt", "updatedAt"]

	// Build column list from first entity and query keys
	if (queryKeys) {
		for (const [field, def] of Object.entries(queryKeys)) {
			if (ignorableFields.includes(field)) {
				continue
			}

			if (isQueryKeyDef(def)) {
				// Replace dots with underscores for nested fields
				const columnName = field.replace(/\./g, "_")
				columns.push(columnName)
				placeholders.push("?")
			}
		}
	}

	// Add the serialized data placeholder
	columns.push("__lazy_data")
	placeholders.push("?")

	// Build the base SQL query
	const sql = `INSERT INTO ${tableName} (${columns.join(", ")})
               VALUES (${placeholders.join(", ")})
               ${buildReturningClause(timestamps)}`

	// Build values array for each entity
	for (const entity of entities) {
		const entityValues: SupportedValueType[] = []

		if (queryKeys) {
			for (const [field, def] of Object.entries(queryKeys)) {
				if (ignorableFields.includes(field)) {
					continue
				}

				if (isQueryKeyDef(def)) {
					const path = field.split(".")
					let value = entity
					for (const key of path) {
						value = value?.[key as keyof typeof value] as T
					}

					if (field in entity || value !== undefined) {
						entityValues.push(toSqliteValue(value as LazyDbValue, def.type))
					}
				}
			}
		}

		// Add placeholder for __lazy_data (will be filled in during execution)
		entityValues.push(new Uint8Array())
		values.push(entityValues)
	}

	return { sql, values }
}

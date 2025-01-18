// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"
import {
	isQueryKeyDef,
	type LazyDbColumnType,
	type EntityType,
	type QueryKeys,
} from "./types.js"

/**
 * Gets a value from a nested object using dot notation
 * @param obj The object to get the value from
 * @param path The dot notation path (e.g., 'address.street')
 * @returns The value at the path
 */
export function getNestedValue(obj: unknown, path: string): unknown {
	if (!obj || typeof obj !== "object") {
		return undefined
	}

	const parts = path.split(".")
	let current: unknown = obj

	for (const part of parts) {
		if (current === null || typeof current !== "object") {
			return undefined
		}
		current = (current as Record<string, unknown>)[part]
	}

	return current
}

/**
 * Sets a value in a nested object using dot notation
 * @param obj The object to set the value in
 * @param path The dot notation path (e.g., 'address.street')
 * @param value The value to set
 */
export function setNestedValue<T extends EntityType>(
	obj: T,
	path: string,
	value: unknown
): void {
	const parts = path.split(".")
	// biome-ignore lint/suspicious/noExplicitAny: <explanation>
	let current: any = obj // Use any here because we need to modify the object

	// Navigate to the parent of the final property
	for (let i = 0; i < parts.length - 1; i++) {
		const part = parts[i]
		if (!(part in current)) {
			current[part] = {}
		}
		current = current[part]
	}

	// Set the final value
	current[parts[parts.length - 1]] = value
}

/**
 * Converts a dot notation path to a valid SQL column name
 * @param path The dot notation path
 * @returns A SQL-safe column name
 */
export function pathToColumnName(path: string): string {
	return path.replace(/\./g, "_")
}

/**
 * Extracts all queryable values from an entity based on query keys
 * @param entity The entity to extract values from
 * @param queryKeys The query keys configuration
 * @returns An object mapping column names to their values
 */
export function extractQueryableValues<T extends EntityType>(
	entity: T,
	queryKeys: QueryKeys<T>
): Record<string, { value: unknown; type: LazyDbColumnType }> {
	const result: Record<string, { value: unknown; type: LazyDbColumnType }> = {}

	for (const [path, def] of Object.entries(queryKeys)) {
		if (!path.includes(".") || !isQueryKeyDef(def)) {
			continue
		}

		const value = getNestedValue(entity, path)
		if (value === undefined && !def.nullable) {
			throw new NodeSqliteError(
				"ERR_SQLITE_CONSTRAINT",
				SqlitePrimaryResultCode.SQLITE_CONSTRAINT,
				"non-null constraint violated",
				`Field "${path}" cannot be null or undefined`,
				undefined
			)
		}

		result[pathToColumnName(path)] = {
			value,
			type: def.type,
		}
	}

	return result
}

/**
 * Converts query keys with dot notation to SQL column definitions
 * @param queryKeys The query keys configuration
 * @returns Array of SQL column definitions
 */
export function createNestedColumnDefinitions<T extends EntityType>(
	queryKeys: QueryKeys<T>
): string[] {
	const columns: string[] = []

	for (const [path, def] of Object.entries(queryKeys)) {
		if (!path.includes(".")) {
			continue // Skip non-nested paths
		}

		const columnName = pathToColumnName(path)
		const constraints: string[] = []

		if (!def.nullable) {
			constraints.push("NOT NULL")
		}

		if (def.unique) {
			constraints.push("UNIQUE")
		}

		if (def.default !== undefined) {
			constraints.push(
				`DEFAULT ${typeof def.default === "string" ? `'${def.default}'` : def.default}`
			)
		}

		columns.push(
			`${columnName} ${def.type}${constraints.length ? ` ${constraints.join(" ")}` : ""}`
		)
	}

	return columns
}

/**
 * Creates index definitions for nested path columns
 * @param tableName The name of the table
 * @param queryKeys The query keys configuration
 * @returns Array of CREATE INDEX statements
 */
export function createNestedIndexDefinitions<T extends EntityType>(
	tableName: string,
	queryKeys: QueryKeys<T>
): string[] {
	const indexes: string[] = []

	for (const [path, def] of Object.entries(queryKeys)) {
		if (!isQueryKeyDef(def) || !path.includes(".")) {
			continue
		}

		const columnName = pathToColumnName(path)
		const indexName = `idx_${tableName}_${columnName}`
		const unique = def.unique ? " UNIQUE" : ""

		indexes.push(
			`CREATE${unique} INDEX IF NOT EXISTS ${indexName} ON ${tableName}(${columnName})`
		)
	}

	return indexes
}

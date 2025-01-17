// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { SupportedValueType } from "node:sqlite"
import {
	type Entity,
	type EntityType,
	isQueryKeyDef,
	type LazyDbValue,
	type QueryKeys,
} from "./types.js"
import type { FindOptions } from "./find.js"
import { buildReturningClause, toSqliteValue } from "./sql.js"
import { buildWhereClause } from "./where.js"
import { extractQueryableValues } from "./paths.js"
import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"

interface UpdateQueryResult {
	sql: string
	params: SupportedValueType[]
}

/**
 * Builds an SQL UPDATE query
 * @param tableName The name of the table to update
 * @param entity The partial entity containing update values
 * @param where The where clause options
 * @param queryKeys Optional query keys configuration
 * @param timestamps Whether to include timestamp fields
 * @returns Object containing the SQL query and parameters
 */
export function buildUpdateQuery<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(
	tableName: string,
	entity: Partial<T>,
	where: Pick<FindOptions<T, QK>, "where">,
	queryKeys?: QK,
	timestamps = false
): UpdateQueryResult {
	const setColumns: string[] = []
	const params: SupportedValueType[] = []
	const ignorableFields = ["_id", "createdAt"]

	// Add updatable query key fields
	if (queryKeys) {
		for (const [field, def] of Object.entries(queryKeys)) {
			if (ignorableFields.includes(field)) {
				continue
			}

			if (!field.includes(".")) {
				const value = entity[field as keyof Partial<T>]
				if (field in entity && isQueryKeyDef(def)) {
					setColumns.push(`${field} = ?`)
					params.push(toSqliteValue(value as LazyDbValue, def.type))
				}
			}
		}

		// Handle nested fields
		const nestedValues = extractQueryableValues(
			entity,
			queryKeys as QueryKeys<Partial<T>>
		)
		for (const [columnName, value] of Object.entries(nestedValues)) {
			setColumns.push(`${columnName} = ?`)
			params.push(value as SupportedValueType)
		}
	}

	// Add __lazy_data placeholder (but don't add a param for it)
	setColumns.push("__lazy_data = ?")

	// Add updatedAt if timestamps are enabled
	if (timestamps) {
		setColumns.push("updatedAt = CURRENT_TIMESTAMP")
	}

	// Build WHERE clause
	const whereResult = where.where
		? buildWhereClause(where.where, queryKeys)
		: { sql: "", params: [] }

	// Build complete SQL
	const sql = `UPDATE ${tableName}
                SET ${setColumns.join(", ")}
                ${whereResult.sql ? `WHERE ${whereResult.sql}` : ""}
                ${buildReturningClause(timestamps)}`

	// Add WHERE clause parameters
	params.push(...whereResult.params)

	return { sql, params }
}

interface UpdateManyQueryResult {
	sql: string
	values: SupportedValueType[][]
}

export function buildUpdateManyQuery<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(
	tableName: string,
	updates: Partial<T>,
	entities: Entity<T>[],
	queryKeys?: QK,
	timestamps = false
): UpdateManyQueryResult {
	const setColumns: string[] = []
	const values: SupportedValueType[][] = []
	const ignorableFields = ["_id", "createdAt"]

	// Build the SET clause columns first
	if (queryKeys) {
		// Handle regular fields
		for (const [field, def] of Object.entries(queryKeys)) {
			if (ignorableFields.includes(field) || !isQueryKeyDef(def)) {
				continue
			}

			if (!field.includes(".") && field in updates) {
				setColumns.push(`${field} = ?`)
			}
		}

		// Handle nested fields
		const nestedFields = Object.keys(queryKeys).filter((k) => k.includes("."))
		for (const field of nestedFields) {
			if (field in updates) {
				const columnName = field.replace(/\./g, "_")
				setColumns.push(`${columnName} = ?`)
			}
		}
	}

	// Add __lazy_data placeholder
	setColumns.push("__lazy_data = ?")

	// Add updatedAt if timestamps are enabled
	if (timestamps) {
		setColumns.push("updatedAt = CURRENT_TIMESTAMP")
	}

	// Build the base SQL query
	const sql = `UPDATE ${tableName}
             SET ${setColumns.join(", ")}
             WHERE _id = ?
             ${buildReturningClause(timestamps)}`

	// Build values array for each entity
	for (const entity of entities) {
		if (!entity?._id) {
			throw new NodeSqliteError(
				"ERR_SQLITE_CONSTRAINT",
				SqlitePrimaryResultCode.SQLITE_CONSTRAINT,
				"non-null constraint violated",
				"Field '_id' cannot be null or undefined",
				undefined
			)
		}
		const entityValues: SupportedValueType[] = []

		// Add SET clause values
		if (queryKeys) {
			for (const [field, def] of Object.entries(queryKeys)) {
				if (ignorableFields.includes(field) || !isQueryKeyDef(def)) {
					continue
				}

				if (!field.includes(".") && field in updates) {
					const value = updates[field as keyof Partial<T>]
					entityValues.push(toSqliteValue(value as LazyDbValue, def.type))
				}
			}
		}

		// Add placeholder for __lazy_data
		entityValues.push(new Uint8Array())

		// Add _id for WHERE clause
		entityValues.push(entity._id)

		values.push(entityValues)
	}

	return { sql, values }
}

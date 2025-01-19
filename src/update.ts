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
import type { PartialDeep } from "type-fest"

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
): {
	sql: string
	params: SupportedValueType[]
	lazyDataIndex: number
} {
	const setColumns: string[] = []
	const params: SupportedValueType[] = []
	const ignorableFields = ["_id", "createdAt"]
	let lazyDataIndex = 0

	// Add regular fields first
	if (queryKeys) {
		for (const [field, def] of Object.entries(queryKeys)) {
			if (
				ignorableFields.includes(field) ||
				field.includes(".") ||
				!isQueryKeyDef(def)
			) {
				continue
			}

			if (field in entity) {
				setColumns.push(`${field} = ?`)
				params.push(
					toSqliteValue(
						entity[field as keyof typeof entity] as LazyDbValue,
						def.type
					)
				)
			}
		}

		// Handle nested fields
		const nestedValues = extractQueryableValues(
			entity,
			queryKeys as QueryKeys<Partial<T>>
		)
		for (const [columnName, { value, type }] of Object.entries(nestedValues)) {
			setColumns.push(`${columnName} = ?`)
			params.push(toSqliteValue(value as LazyDbValue, type))
		}
	}

	// Record the index where __lazy_data param will go
	lazyDataIndex = params.length

	// Add __lazy_data placeholder
	setColumns.push("__lazy_data = ?")

	// Add updatedAt if timestamps enabled
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

	return { sql, params, lazyDataIndex }
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
	updates: PartialDeep<T>,
	entities: Entity<T>[],
	queryKeys?: QK,
	timestamps = false
): UpdateManyQueryResult {
	const setColumns: string[] = []
	const values: SupportedValueType[][] = []
	const ignorableFields = ["_id", "createdAt"]

	// Build the SET clause columns first
	if (queryKeys) {
		// Handle regular fields first
		for (const [field, def] of Object.entries(queryKeys)) {
			if (ignorableFields.includes(field) || !isQueryKeyDef(def)) {
				continue
			}

			if (!field.includes(".")) {
				const value = updates[field as keyof PartialDeep<T>]
				if (value !== undefined) {
					setColumns.push(`${field} = ?`)
				}
			}
		}

		// Handle nested fields
		for (const [field, def] of Object.entries(queryKeys)) {
			if (!isQueryKeyDef(def) || !field.includes(".")) {
				continue
			}

			// Split the field path and navigate the updates object
			const parts = field.split(".")
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
			let currentValue: any = updates
			let isDefined = true

			for (const part of parts) {
				if (currentValue === undefined || !(part in currentValue)) {
					isDefined = false
					break
				}
				currentValue = currentValue[part]
			}

			if (isDefined) {
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

		// Add SET clause values for regular fields
		if (queryKeys) {
			for (const [field, def] of Object.entries(queryKeys)) {
				if (ignorableFields.includes(field) || !isQueryKeyDef(def)) {
					continue
				}

				if (!field.includes(".")) {
					const value = updates[field as keyof PartialDeep<T>]
					if (value !== undefined) {
						entityValues.push(toSqliteValue(value as LazyDbValue, def.type))
					}
				}
			}

			// Add SET clause values for nested fields
			for (const [field, def] of Object.entries(queryKeys)) {
				if (!isQueryKeyDef(def) || !field.includes(".")) {
					continue
				}

				const parts = field.split(".")
				// biome-ignore lint/suspicious/noExplicitAny: <explanation>
				let currentValue: any = updates
				let isDefined = true

				for (const part of parts) {
					if (currentValue === undefined || !(part in currentValue)) {
						isDefined = false
						break
					}
					currentValue = currentValue[part]
				}

				if (isDefined) {
					entityValues.push(
						toSqliteValue(currentValue as LazyDbValue, def.type)
					)
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

// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { SupportedValueType } from "node:sqlite"
import {
	type EntityType,
	isQueryKeyDef,
	type LazyDbValue,
	type QueryKeys,
} from "./types.js"
import type { FindOptions } from "./find.js"
import { buildReturningClause, toSqliteValue } from "./sql.js"
import { buildWhereClause } from "./where.js"

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
export function buildUpdateQuery<T extends EntityType, QK extends QueryKeys<T>>(
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

			const value = entity[field as keyof Partial<T>]
			if (field in entity && isQueryKeyDef(def)) {
				setColumns.push(`${field} = ?`)
				params.push(toSqliteValue(value as LazyDbValue, def.type))
			}
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

/**
 * Builds an SQL UPDATE query for multiple records
 * @param tableName The name of the table to update
 * @param updates The fields to update
 * @param where The where clause options
 * @param queryKeys Optional query keys configuration
 * @param timestamps Whether to include timestamp fields
 * @returns Object containing the SQL query and parameters
 */
export function buildUpdateManyQuery<
	T extends EntityType,
	QK extends QueryKeys<T>,
>(
	tableName: string,
	updates: Partial<T>,
	where: Pick<FindOptions<T, QK>, "where">,
	queryKeys?: QK,
	timestamps = false
): UpdateQueryResult {
	if (!queryKeys) {
		return { sql: "", params: [] }
	}

	const setColumns: string[] = []
	const updateParams: SupportedValueType[] = []

	// Build SET clause for all update fields except system fields
	for (const [field, value] of Object.entries(updates)) {
		if (!["_id", "createdAt", "updatedAt"].includes(field)) {
			setColumns.push(`${field} = ?`)
			updateParams.push(value as SupportedValueType)
		}
	}

	// Add updatedAt if timestamps are enabled
	if (timestamps) {
		setColumns.push("updatedAt = CURRENT_TIMESTAMP")
	}

	// If no fields to update and timestamps not enabled, error
	if (setColumns.length === 0) {
		throw new Error("No fields to update")
	}

	// Build WHERE clause
	const whereResult = where.where
		? buildWhereClause(where.where, queryKeys)
		: { sql: "", params: [] }

	// Build the complete SQL
	const sql = `UPDATE ${tableName} SET ${setColumns.join(", ")}${whereResult.sql ? ` WHERE ${whereResult.sql}` : ""}`

	// Combine parameters in correct order: first update values, then where params
	const params = [...updateParams]
	if (whereResult.params.length > 0) {
		params.push(...whereResult.params)
	}

	return { sql, params }
}

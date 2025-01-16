// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { SupportedValueType } from "node:sqlite"
import type { EntityType, QueryKeys } from "./types.js"
import type { FindOptions } from "./find.js"
import { buildWhereClause } from "./where.js"

interface DeleteQueryResult {
	sql: string
	params: SupportedValueType[]
}

/**
 * Builds an SQL DELETE query for a single entity
 *
 * @param tableName The name of the table to delete from
 * @param options The where clause options
 * @param queryKeys Optional query keys configuration
 * @returns Object containing the SQL query and parameters
 */
export function buildDeleteQuery<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(
	tableName: string,
	options: Pick<FindOptions<T, QK>, "where">,
	queryKeys?: QK
): DeleteQueryResult {
	// Build WHERE clause
	const whereResult = options.where
		? buildWhereClause(options.where, queryKeys)
		: { sql: "", params: [] }

	// Build the complete SQL query
	const sql = `DELETE FROM ${tableName}${
		whereResult.sql ? ` WHERE ${whereResult.sql}` : ""
	} LIMIT 1`

	return {
		sql,
		params: whereResult.params,
	}
}

/**
 * Builds an SQL DELETE query for multiple entities
 *
 * @param tableName The name of the table to delete from
 * @param options The where clause options
 * @param queryKeys Optional query keys configuration
 * @returns Object containing the SQL query and parameters
 */
export function buildDeleteManyQuery<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(
	tableName: string,
	options: Pick<FindOptions<T, QK>, "where">,
	queryKeys?: QK
): DeleteQueryResult {
	// Build WHERE clause
	const whereResult = options.where
		? buildWhereClause(options.where, queryKeys)
		: { sql: "", params: [] }

	// Build the complete SQL query
	const sql = `DELETE FROM ${tableName}${
		whereResult.sql ? ` WHERE ${whereResult.sql}` : ""
	}`

	return {
		sql,
		params: whereResult.params,
	}
}

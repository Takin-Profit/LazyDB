// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { SupportedValueType } from "node:sqlite"
import type { EntityType, QueryKeys } from "./types.js"
import {
	array,
	bool,
	literal,
	num,
	object,
	optional,
	record,
	string,
	union,
} from "./utils.js"
import { buildWhereClause, Where } from "./where.js"

// Update the FindOptionsSchema
export const FindOptions = object({
	where: optional(Where),
	limit: optional(num()),
	offset: optional(num()),
	orderBy: optional(record(string(), union([literal("ASC"), literal("DESC")]))),
	distinct: optional(bool()),
	groupBy: optional(array(string())),
})

export type FindOptions<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
> = {
	where?: Where<T, QK>
	limit?: number
	offset?: number
	orderBy?: Partial<Record<keyof QK, "ASC" | "DESC">>
	distinct?: boolean
	groupBy?: Array<keyof QK>
}

export function isGroupByArray<Q extends QueryKeys<unknown>>(
	groupBy: unknown
): groupBy is Array<keyof Q> {
	return (
		Array.isArray(groupBy) &&
		(groupBy as Array<unknown>).length > 0 &&
		(groupBy as Array<unknown>).every((key) => typeof key === "string")
	)
}

export interface FindQueryResult {
	sql: string
	params: SupportedValueType[]
}

export function buildFindQuery<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(
	tableName: string,
	options: FindOptions<T, QK>,
	queryKeys?: QK
): FindQueryResult {
	// Start building the SQL query
	let sql = `SELECT * FROM ${tableName}`
	const params: SupportedValueType[] = []

	// Add WHERE clause if provided
	if (options.where) {
		const whereClause = buildWhereClause(options.where, queryKeys)
		if (whereClause.sql) {
			sql += ` WHERE ${whereClause.sql}`
			params.push(...whereClause.params)
		}
	}

	// Add GROUP BY clause if provided
	if (options.groupBy && isGroupByArray(options.groupBy)) {
		sql += ` GROUP BY ${(options.groupBy as Array<unknown>).join(", ")}`
	}

	// Add ORDER BY clause if provided
	if (options.orderBy) {
		const orderClauses = Object.entries(options.orderBy).map(
			([column, direction]) => `${column} ${direction}`
		)
		if (orderClauses.length > 0) {
			sql += ` ORDER BY ${orderClauses.join(", ")}`
		}
	}

	// Add LIMIT and OFFSET if provided
	if (options.limit !== undefined) {
		sql += " LIMIT ?"
		params.push(options.limit)

		if (options.offset !== undefined) {
			sql += " OFFSET ?"
			params.push(options.offset)
		}
	}

	// Add DISTINCT if requested
	if (options.distinct) {
		sql = sql.replace("SELECT *", "SELECT DISTINCT *")
	}

	return { sql, params }
}

// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { SupportedValueType } from "node:sqlite"
import type { EntityType, QueryKeys } from "./types.js"
import { buildWhereClause, type Where } from "./where.js"
import {
	isValidationErrs,
	validationErr,
	type ValidationError,
} from "./validate.js"
import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"

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

/**
 * Validates FindOptions to ensure that limit, offset, distinct, orderBy, groupBy, etc.
 * are of the correct type and reference valid query keys.
 *
 * @param options The user-provided FindOptions
 * @param queryKeys (Optional) The query keys to validate groupBy/orderBy columns against
 * @returns An array of ValidationErrors (empty if valid)
 */
export function validateFindOptions<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(options: unknown, queryKeys?: QK): ValidationError[] {
	const errors: ValidationError[] = []

	// Ensure options is an object
	if (typeof options !== "object" || options === null) {
		errors.push(validationErr({ msg: "FindOptions must be an object" }))
		return errors
	}

	// Safely cast
	const opts = options as FindOptions<T, QK>

	// Validate limit
	if (
		opts.limit !== undefined &&
		(typeof opts.limit !== "number" || opts.limit < 0)
	) {
		errors.push(
			validationErr({
				msg: "limit must be a non-negative number",
				path: "limit",
			})
		)
	}

	// Validate offset
	if (
		opts.offset !== undefined &&
		(typeof opts.offset !== "number" || opts.offset < 0)
	) {
		errors.push(
			validationErr({
				msg: "offset must be a non-negative number",
				path: "offset",
			})
		)
	}

	// Validate distinct
	if (opts.distinct !== undefined && typeof opts.distinct !== "boolean") {
		errors.push(
			validationErr({
				msg: "distinct must be a boolean",
				path: "distinct",
			})
		)
	}

	// Validate groupBy
	if (opts.groupBy !== undefined) {
		if (!Array.isArray(opts.groupBy)) {
			errors.push(
				validationErr({
					msg: "groupBy must be an array",
					path: "groupBy",
				})
			)
		} else if (opts.groupBy.some((key) => typeof key !== "string")) {
			errors.push(
				validationErr({
					msg: "groupBy must be an array of string keys",
					path: "groupBy",
				})
			)
		} else if (queryKeys) {
			// If queryKeys are provided, ensure each groupBy key is valid
			for (const key of opts.groupBy) {
				if (!(key in queryKeys)) {
					errors.push(
						validationErr({
							msg: `Invalid groupBy key: ${String(key)}`,
							path: `groupBy.${String(key)}`,
						})
					)
				}
			}
		}
	}

	// Validate orderBy
	if (opts.orderBy !== undefined) {
		if (typeof opts.orderBy !== "object" || opts.orderBy === null) {
			errors.push(
				validationErr({
					msg: "orderBy must be an object",
					path: "orderBy",
				})
			)
		} else {
			for (const [column, direction] of Object.entries(opts.orderBy)) {
				// direction must be "ASC" or "DESC"
				if (direction !== "ASC" && direction !== "DESC") {
					errors.push(
						validationErr({
							msg: `Invalid sort direction for ${column}. Must be "ASC" or "DESC".`,
							path: `orderBy.${column}`,
						})
					)
				}
				// If queryKeys exist, ensure `column` is a valid key
				if (queryKeys && !(column in queryKeys)) {
					errors.push(
						validationErr({
							msg: `Invalid orderBy key: ${column}`,
							path: `orderBy.${column}`,
						})
					)
				}
			}
		}
	}

	return errors
}

export function buildFindQuery<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(
	tableName: string,
	options: FindOptions<T, QK>,
	queryKeys?: QK
): FindQueryResult {
	const result = validateFindOptions<T, QK>(options, queryKeys)

	if (isValidationErrs(result)) {
		throw new NodeSqliteError(
			"ERR_SQLITE_ERROR",
			SqlitePrimaryResultCode.SQLITE_ERROR,
			"Validation error",
			`Invalid FindOptions: ${result.map((err) => err.message).join(", ")}`
		)
	}

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

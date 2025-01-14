// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"
import { LazyDbValue, NodeSqliteValue, type QueryKeyDef } from "./types.js"
import {
	type $,
	array,
	isValidationErrors,
	literal,
	object,
	string,
	toSqliteValue,
	tuple,
	union,
	validate,
} from "./utils.js"

const ComparisonOperator = union([
	literal("="),
	literal("!="),
	literal(">"),
	literal("<"),
	literal(">="),
	literal("<="),
	literal("LIKE"),
	literal("NOT LIKE"),
	literal("IN"),
	literal("NOT IN"),
	literal("IS"),
	literal("IS NOT"),
])

export type ComparisonOperator = $<typeof ComparisonOperator>

const LogicalOperator = union([literal("AND"), literal("OR")])

export type LogicalOperator = $<typeof LogicalOperator>

export const WhereCondition = tuple([
	string(),
	ComparisonOperator,
	union([LazyDbValue, array(LazyDbValue)]),
])

type InOperator = "IN" | "NOT IN"
type NonInOperator = Exclude<ComparisonOperator, InOperator>

export type WhereCondition<T> = {
	[K in keyof T]: [K, InOperator, T[K][]] | [K, NonInOperator, T[K]]
}[keyof T]

const WhereClauseResult = object({
	sql: string(),
	params: array(NodeSqliteValue),
})

/**
 * Result of building a WHERE clause
 */
export type WhereClauseResult = $<typeof WhereClauseResult>

const ComplexWhereCondition = union([
	tuple([WhereCondition]),
	tuple([WhereCondition, LogicalOperator, WhereCondition]),
	tuple([
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
	]),
	tuple([
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
	]),
	tuple([
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
	]),
	tuple([
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
	]),
	tuple([
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
	]),
	tuple([
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
	]),
	tuple([
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
	]),
	tuple([
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
		LogicalOperator,
		WhereCondition,
	]),
])

type ComplexWhereCondition<T> =
	| [WhereCondition<T>]
	| [WhereCondition<T>, LogicalOperator, WhereCondition<T>]
	| [
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
	  ]
	| [
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
	  ]
	| [
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
	  ]
	| [
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
	  ]
	| [
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
	  ]
	| [
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
	  ]
	| [
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
	  ]
	| [
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
			LogicalOperator,
			WhereCondition<T>,
	  ]

export const Where = union([WhereCondition, ComplexWhereCondition])

/**
 * Union type for all possible WHERE clause inputs
 */
export type Where<T> = WhereCondition<T> | ComplexWhereCondition<T>

function handleSingleCondition<T>(
	where: WhereCondition<T>,
	queryKeys?: Record<string, QueryKeyDef>
): WhereClauseResult & { fields: string[] } {
	const [field, operator, value] = where
	const columnType =
		typeof field === "string" && queryKeys?.[field]?.type
			? queryKeys[field].type
			: "TEXT" // Default to TEXT if no type is found

	if (operator === "IN" || operator === "NOT IN") {
		if (!Array.isArray(value)) {
			throw new NodeSqliteError(
				"ERR_SQLITE_WHERE",
				SqlitePrimaryResultCode.SQLITE_MISUSE,
				"Invalid IN/NOT IN value",
				`Operator ${operator} requires an array value`,
				undefined
			)
		}

		const convertedParams = value.map((v) =>
			toSqliteValue(v as LazyDbValue, columnType)
		)
		const placeholders = convertedParams.map(() => "?").join(", ")
		return {
			sql: `${String(field)} ${operator} (${placeholders})`,
			params: convertedParams,
			fields: [String(field)],
		}
	}

	if (operator === "IS" || operator === "IS NOT") {
		if (value !== null) {
			throw new NodeSqliteError(
				"ERR_SQLITE_WHERE",
				SqlitePrimaryResultCode.SQLITE_MISUSE,
				"Invalid IS/IS NOT value",
				`Operator ${operator} only works with NULL values`,
				undefined
			)
		}
		return {
			sql: `${String(field)} ${operator} NULL`,
			params: [],
			fields: [String(field)],
		}
	}

	const convertedValue = toSqliteValue(value as LazyDbValue, columnType)
	return {
		sql: `${String(field)} ${operator} ?`,
		params: [convertedValue],
		fields: [String(field)],
	}
}

function handleComplexCondition<T>(
	where: ComplexWhereCondition<T>,
	queryKeys?: Record<string, QueryKeyDef>
): WhereClauseResult & { fields: string[] } {
	const parts: string[] = []
	const params: NodeSqliteValue[] = []
	const fields: string[] = []
	let currentOperator = "AND"

	for (let i = 0; i < where.length; i++) {
		const item = where[i]

		if (i % 2 === 1) {
			// Odd indices should be operators
			currentOperator = item as LogicalOperator
			continue
		}

		// Even indices should be conditions
		const condition = item as Where<T>
		const {
			sql,
			params: conditionParams,
			fields: conditionFields,
		} = buildWhereClause(condition, queryKeys) // Recursive call
		parts.push(sql)
		params.push(...conditionParams)
		fields.push(...conditionFields)
	}

	return {
		sql:
			parts.length > 1 ? `(${parts.join(` ${currentOperator} `)})` : parts[0],
		params,
		fields,
	}
}

function isWhereCondition<T>(where: Where<T>): where is WhereCondition<T> {
	return (
		Array.isArray(where) &&
		where.length === 3 &&
		typeof where[0] === "string" &&
		typeof where[1] === "string" // Assuming operator is a string
	)
}

export function buildWhereClause<T>(
	where: Where<T>,
	queryKeys?: Record<string, QueryKeyDef>
): WhereClauseResult & { fields: string[] } {
	// Validate the where condition first
	const validationResult = validate(Where, where)
	if (isValidationErrors(validationResult)) {
		throw new NodeSqliteError(
			"ERR_SQLITE_WHERE",
			SqlitePrimaryResultCode.SQLITE_MISUSE,
			"Invalid where clause",
			`Where clause validation failed: ${validationResult.map((e) => e.message).join(", ")}`,
			undefined
		)
	}

	// Use the type guard to determine the type of condition
	if (isWhereCondition(where)) {
		return handleSingleCondition(where, queryKeys)
	}
	return handleComplexCondition(where as ComplexWhereCondition<T>, queryKeys)
}

// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"
import { toSqliteValue } from "./sql.js"
import {
	type DotPathValue,
	type EntityType,
	LazyDbValue,
	NodeSqliteValue,
	type QueryKeys,
	type RepositoryOptions,
} from "./types.js"
import {
	type $,
	array,
	isValidationErrors,
	literal,
	object,
	string,
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

// Helper type to get keys that are actually defined in queryKeys
export type QueryableKeys<
	T extends EntityType,
	QK = NonNullable<RepositoryOptions<T>["queryKeys"]>,
> = keyof QK & string

// Helper type to get the value type for a queryable key
export type QueryableValue<T, K extends string> = DotPathValue<T, K>

export type LogicalOperator = $<typeof LogicalOperator>

export const WhereCondition = tuple([
	string(),
	ComparisonOperator,
	union([LazyDbValue, array(LazyDbValue)]),
])

type InOperator = "IN" | "NOT IN"
type NullOperator = "IS" | "IS NOT"
type NonInOperator = Exclude<ComparisonOperator, InOperator | NullOperator>

type ValidKeys<QK> = keyof QK & string

// The core WhereCondition type that ensures only valid fields are used
export type WhereCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
> = {
	[K in ValidKeys<QK>]:
		| [K, InOperator, DotPathValue<T, K>[]]
		| [K, NonInOperator, DotPathValue<T, K>]
		| [K, NullOperator, null]
}[ValidKeys<QK>]

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

type ComplexWhereCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
> =
	| [WhereCondition<T, QK>]
	| [WhereCondition<T, QK>, LogicalOperator, WhereCondition<T, QK>]
	| [
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
	  ]
	| [
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
	  ]
	| [
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
	  ]
	| [
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
	  ]
	| [
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
	  ]
	| [
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
	  ]
	| [
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
	  ]
	| [
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
			LogicalOperator,
			WhereCondition<T, QK>,
	  ]

export const Where = union([WhereCondition, ComplexWhereCondition])

/**
 * Union type for all possible WHERE clause inputs
 */
export type Where<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
> = WhereCondition<T, QK> | ComplexWhereCondition<T, QK>

// And modify handleSingleCondition to use them both
function handleSingleCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(
	where: WhereCondition<T, QK>,
	queryKeys?: QK
): WhereClauseResult & { fields: string[] } {
	const [field, operator, value] = where

	if (!queryKeys || !queryKeys[field]) {
		throw new NodeSqliteError(
			"ERR_SQLITE_WHERE",
			SqlitePrimaryResultCode.SQLITE_MISUSE,
			"Unknown field in where clause",
			`Field "${String(field)}" is not defined in queryKeys`,
			undefined
		)
	}

	const columnType = queryKeys[field].type

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

		if (value.length === 0) {
			throw new NodeSqliteError(
				"ERR_SQLITE_WHERE",
				SqlitePrimaryResultCode.SQLITE_MISUSE,
				"Invalid IN/NOT IN value",
				"Empty arrays are not allowed in IN/NOT IN clauses",
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

function handleComplexCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(
	where: ComplexWhereCondition<T, QK>,
	queryKeys: QK
): WhereClauseResult & { fields: string[] } {
	const parts: string[] = []
	const operators: LogicalOperator[] = []
	const params: NodeSqliteValue[] = []
	const fields: string[] = []

	for (let i = 0; i < where.length; i++) {
		const item = where[i]

		if (i % 2 === 1) {
			// Odd indices are operators, store them in sequence
			operators.push(item as LogicalOperator)
			continue
		}

		// Even indices are conditions
		const condition = item as Where<T, QK>
		const {
			sql,
			params: conditionParams,
			fields: conditionFields,
		} = buildWhereClause(condition, queryKeys)
		parts.push(sql)
		params.push(...conditionParams)
		fields.push(...conditionFields)
	}

	// Combine parts with their respective operators
	const combinedSql = parts.reduce((acc, part, idx) => {
		if (idx === 0) {
			return part
		}
		return `${acc} ${operators[idx - 1]} ${part}`
	}, "")

	return {
		sql: parts.length > 1 ? `(${combinedSql})` : parts[0],
		params,
		fields,
	}
}

function isWhereCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(where: Where<T, QK>): where is WhereCondition<T, QK> {
	return (
		Array.isArray(where) &&
		where.length === 3 &&
		typeof where[0] === "string" &&
		typeof where[1] === "string" // Assuming operator is a string
	)
}

export function buildWhereClause<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(
	where: Where<T, QK>,
	queryKeys?: QK
): WhereClauseResult & { fields: string[] } {
	// If no queryKeys, return empty result since there are no queryable fields
	if (!queryKeys) {
		return {
			sql: "",
			params: [],
			fields: [],
		}
	}

	// Rest of validation and processing
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

	if (isWhereCondition(where)) {
		return handleSingleCondition(where, queryKeys)
	}
	return handleComplexCondition(where, queryKeys)
}

// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"
import { pathToColumnName } from "./paths.js"
import { toSqliteValue } from "./sql.js"
import type {
	DotPathValue,
	EntityType,
	LazyDbValue,
	NodeSqliteValue,
	QueryKeys,
	RepositoryOptions,
} from "./types.js"

const COMPARISON_OPERATORS = [
	"=",
	"!=",
	">",
	"<",
	">=",
	"<=",
	"LIKE",
	"NOT LIKE",
	"IN",
	"NOT IN",
	"IS",
	"IS NOT",
] as const

const LOGICAL_OPERATORS = ["AND", "OR"] as const

export type ComparisonOperator = (typeof COMPARISON_OPERATORS)[number]
export type LogicalOperator = (typeof LOGICAL_OPERATORS)[number]

// Helper type to get keys that are actually defined in queryKeys
export type QueryableKeys<
	T extends EntityType,
	QK = NonNullable<RepositoryOptions<T>["queryKeys"]>,
> = keyof QK & string

// Helper type to get the value type for a queryable key
export type QueryableValue<T, K extends string> = DotPathValue<T, K>

type InOperator = "IN" | "NOT IN"
type NullOperator = "IS" | "IS NOT"
type NonInOperator = Exclude<ComparisonOperator, InOperator | NullOperator>

type ValidKeys<QK> = keyof QK & string

type SystemFieldValueMap = {
	_id: number
	createdAt: string
	updatedAt: string
	__lazy_data: Uint8Array
}

type GetFieldType<
	T,
	K extends string,
	_QK,
> = K extends keyof SystemFieldValueMap
	? SystemFieldValueMap[K]
	: DotPathValue<T, K>

type SimpleWhereCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
> = {
	[K in ValidKeys<QK>]:
		| [K, InOperator, GetFieldType<T, K, QK>[]]
		| [K, NonInOperator, GetFieldType<T, K, QK>]
		| [K, NullOperator, null]
}[ValidKeys<QK>]

type BooleanClause<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
> = SimpleWhereCondition<T, QK> | ComplexWhereCondition<T, QK>

export type WhereClauseResult = {
	sql: string
	params: NodeSqliteValue[]
	fields: string[]
}

// Type for arrays of conditions joined by logical operators
type WhereArray<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
> = [
	SimpleWhereCondition<T, QK> | ComplexWhereCondition<T, QK>,
	...(
		| LogicalOperator
		| SimpleWhereCondition<T, QK>
		| ComplexWhereCondition<T, QK>
	)[],
]

export type ComplexWhereCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
> = WhereArray<T, QK>

export type Where<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
> = SimpleWhereCondition<T, QK> | ComplexWhereCondition<T, QK>

// Validation helper functions
function isComparisonOperator(value: unknown): value is ComparisonOperator {
	return (
		typeof value === "string" &&
		COMPARISON_OPERATORS.includes(value as ComparisonOperator)
	)
}

function isLogicalOperator(value: unknown): value is LogicalOperator {
	return (
		typeof value === "string" &&
		LOGICAL_OPERATORS.includes(value as LogicalOperator)
	)
}

function isValidFieldName<QK extends QueryKeys<unknown>>(
	field: unknown,
	queryKeys: QK
): field is keyof QK {
	return typeof field === "string" && field in queryKeys
}

function validateWhereValue(
	value: unknown,
	operator: ComparisonOperator
): value is LazyDbValue | LazyDbValue[] {
	if (operator === "IN" || operator === "NOT IN") {
		return Array.isArray(value) && value.length > 0
	}
	if (operator === "IS" || operator === "IS NOT") {
		return value === null
	}
	return value !== undefined && value !== null
}

function validateSimpleCondition<T extends EntityType, QK extends QueryKeys<T>>(
	condition: unknown,
	queryKeys: QK
): condition is SimpleWhereCondition<T, QK> {
	if (!Array.isArray(condition) || condition.length !== 3) {
		return false
	}

	const [field, operator, value] = condition

	if (!isValidFieldName(field, queryKeys)) {
		throw new NodeSqliteError(
			"ERR_SQLITE_WHERE",
			SqlitePrimaryResultCode.SQLITE_MISUSE,
			"Invalid field name",
			`Field "${String(field)}" is not defined in queryKeys`,
			undefined
		)
	}

	if (!isComparisonOperator(operator)) {
		throw new NodeSqliteError(
			"ERR_SQLITE_WHERE",
			SqlitePrimaryResultCode.SQLITE_MISUSE,
			"Invalid operator",
			`Operator "${String(operator)}" is not a valid comparison operator`,
			undefined
		)
	}

	if (!validateWhereValue(value, operator)) {
		throw new NodeSqliteError(
			"ERR_SQLITE_WHERE",
			SqlitePrimaryResultCode.SQLITE_MISUSE,
			"Invalid value",
			`Invalid value for operator "${operator}"`,
			undefined
		)
	}

	return true
}

function validateComplexCondition<
	T extends EntityType,
	QK extends QueryKeys<T>,
>(where: unknown[]): where is ComplexWhereCondition<T, QK> {
	if (where.length < 3 || where.length % 2 === 0) {
		return false
	}

	// Check operators at odd indices
	for (let i = 1; i < where.length; i += 2) {
		if (!isLogicalOperator(where[i])) {
			return false
		}
	}

	// Check conditions at even indices
	for (let i = 0; i < where.length; i += 2) {
		const item = where[i]
		if (!Array.isArray(item)) {
			return false
		}
	}

	return true
}

// Type guard for simple where conditions
function isSimpleWhereCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(where: BooleanClause<T, QK>): where is SimpleWhereCondition<T, QK> {
	return (
		Array.isArray(where) &&
		where.length === 3 &&
		typeof where[0] === "string" &&
		typeof where[1] === "string"
	)
}

function handleBooleanClause<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(clause: BooleanClause<T, QK>, queryKeys: QK): WhereClauseResult {
	if (isSimpleWhereCondition(clause)) {
		return handleSingleCondition(clause, queryKeys)
	}
	return handleComplexCondition(clause, queryKeys)
}

function handleSingleCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(where: SimpleWhereCondition<T, QK>, queryKeys?: QK): WhereClauseResult {
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
	const columnName = field.replace(/\./g, "_")

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
			sql: `${columnName} ${operator} (${placeholders})`,
			params: convertedParams,
			fields: [columnName],
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
			sql: `${columnName} ${operator} NULL`,
			params: [],
			fields: [columnName],
		}
	}

	const convertedValue = toSqliteValue(value as LazyDbValue, columnType)
	return {
		sql: `${columnName} ${operator} ?`,
		params: [convertedValue],
		fields: [columnName],
	}
}

function handleComplexCondition<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(where: ComplexWhereCondition<T, QK>, queryKeys: QK): WhereClauseResult {
	const parts: string[] = []
	const params: NodeSqliteValue[] = []
	const fields: string[] = []

	// Handle all elements in the array
	for (let i = 0; i < where.length; i++) {
		const item = where[i]

		// If it's an operator (odd indices), skip
		if (i % 2 === 1) {
			continue
		}

		// Handle the condition (even indices)
		if (Array.isArray(item)) {
			if (item.length === 3 && typeof item[0] === "string") {
				// Simple condition
				const result = handleSingleCondition(
					item as SimpleWhereCondition<T, QK>,
					queryKeys
				)
				parts.push(result.sql)
				params.push(...result.params)
				fields.push(...result.fields)
			} else {
				// Nested condition
				const result = handleBooleanClause(
					item as BooleanClause<T, QK>,
					queryKeys
				)
				parts.push(result.sql)
				params.push(...result.params)
				fields.push(...result.fields)
			}
		}
	}

	// Combine with operators
	let sql = parts[0]
	for (let i = 1; i < parts.length; i++) {
		const operator = where[i * 2 - 1] as LogicalOperator
		sql += ` ${operator} ${parts[i]}`
	}

	return {
		sql: parts.length > 1 ? `(${sql})` : sql,
		params,
		fields,
	}
}

export function buildWhereClause<
	T extends EntityType,
	QK extends QueryKeys<T> = QueryKeys<T>,
>(where?: Where<T, QK>, queryKeys?: QK): WhereClauseResult {
	if (!where) {
		return { sql: "", params: [], fields: [] }
	}
	if (!queryKeys) {
		return { sql: "", params: [], fields: [] }
	}

	if (!Array.isArray(where)) {
		throw new NodeSqliteError(
			"ERR_SQLITE_WHERE",
			SqlitePrimaryResultCode.SQLITE_MISUSE,
			"Invalid where clause",
			"Where clause must be an array",
			undefined
		)
	}

	if (where.length === 3 && typeof where[0] === "string") {
		if (!validateSimpleCondition<T, QK>(where, queryKeys)) {
			throw new NodeSqliteError(
				"ERR_SQLITE_WHERE",
				SqlitePrimaryResultCode.SQLITE_MISUSE,
				"Invalid where clause",
				"Invalid simple condition structure",
				undefined
			)
		}
		return handleSingleCondition(where, queryKeys)
	}

	if (!validateComplexCondition<T, QK>(where)) {
		throw new NodeSqliteError(
			"ERR_SQLITE_WHERE",
			SqlitePrimaryResultCode.SQLITE_MISUSE,
			"Invalid where clause",
			"Invalid complex condition structure",
			undefined
		)
	}

	return handleComplexCondition(where, queryKeys)
}

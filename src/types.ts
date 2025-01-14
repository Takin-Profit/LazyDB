// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { PragmaConfig } from "./pragmas.js"
import { StatementCacheOptions } from "./cache.js"
import {
	type $,
	any,
	array,
	bool,
	func,
	literal,
	nil,
	num,
	object,
	optional,
	record,
	string,
	tuple,
	uint8Array,
	union,
	unit,
	unknown,
	validate,
} from "./utils.js"
import type { StatementSync } from "node:sqlite"
import type { GroupByTuples } from "./group-by.js"

// Define the SQLite column types
const SQLiteColumnType = union([
	literal("TEXT"),
	literal("INTEGER"),
	literal("REAL"),
	literal("BLOB"),
	literal("BOOLEAN"),
])

export type SqliteColumnType = $<typeof SQLiteColumnType>

const QueryKeyDef = object({
	type: SQLiteColumnType,
	index: optional(union([literal(true), object({ unique: literal(true) })])),
	nullable: optional(literal(true)),
	default: optional(any()),
})

export type QueryKeyDef = $<typeof QueryKeyDef>

export const QueryKeys = object({
	queryKeys: record(string(), QueryKeyDef),
})

export const validateQueryKeys = (data: unknown) => validate(QueryKeys, data)

export type Entity<T> = {
	_id?: number
} & T

export const SerializerOptions = union([
	literal("json"),
	literal("msgpack"),
	object({
		encode: func([unknown()], uint8Array()),
		decode: func([uint8Array()], unknown()),
	}),
])

export type SerializerOptions = $<typeof SerializerOptions>

export const DatabaseOptions = object({
	location: string(),
	timestamps: optional(bool()),
	serializer: optional(SerializerOptions),
	pragma: optional(PragmaConfig),
	environment: optional(
		union([literal("development"), literal("testing"), literal("production")])
	),
	logger: optional(func([string()], unit())),
	statementCache: optional(union([literal(true), StatementCacheOptions])),
})

export type DatabaseOptions = $<typeof DatabaseOptions>

export const RepositoryOptions = object({
	prepareStatement: func([string()], unknown()),
	timestamps: optional(bool()),
	queryKeys: optional(QueryKeys),
	serializer: object({
		encode: func([unknown()], uint8Array()),
		decode: func([uint8Array()], unknown()),
	}),
	logger: optional(func([string()], unit())),
})
export type RepositoryOptions<T extends { [key: string]: unknown }> = Readonly<{
	prepareStatement: (sql: string) => StatementSync
	timestamps?: boolean
	queryKeys?: {
		[K in keyof T]?: QueryKeyDef
	}
	serializer: {
		encode: (obj: unknown) => Uint8Array
		decode: (buf: Uint8Array) => unknown
	}
	logger?: (msg: string) => void
}>

export type CreateRepositoryOptions<T> = Readonly<{
	timestamps?: boolean
	queryKeys?: {
		[K in keyof T]?: QueryKeyDef
	}
	logger?: (msg: string) => void
}>

const SupportedValue = union([
	string(),
	num(),
	bool(),
	nil(),
	uint8Array(),
	array(union([string(), num(), bool(), nil()])),
])

/**
 * SQLite supported value types for WHERE conditions
 */
export type SupportedValue = $<typeof SupportedValue>

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
	union([SupportedValue, array(SupportedValue)]),
])

type InOperator = "IN" | "NOT IN"
type NonInOperator = Exclude<ComparisonOperator, InOperator>

export type WhereCondition<T> = {
	[K in keyof T]: [K, InOperator, T[K][]] | [K, NonInOperator, T[K]]
}[keyof T]

const WhereClauseResult = object({
	sql: string(),
	params: array(SupportedValue),
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

// Update the FindOptionsSchema
export const FindOptions = object({
	where: optional(Where),
	limit: optional(num()),
	offset: optional(num()),
	orderBy: optional(record(string(), union([literal("ASC"), literal("DESC")]))),
	distinct: optional(bool()),
	groupBy: optional(array(string())),
})

export type FindOptions<T extends { [key: string]: unknown }> = {
	where?: Where<T>
	limit?: number
	offset?: number
	orderBy?: T extends {
		[K in keyof Required<
			NonNullable<RepositoryOptions<T>["queryKeys"]>
		>]: unknown
	}
		? Partial<
				Record<
					keyof NonNullable<RepositoryOptions<T>["queryKeys"]>,
					"ASC" | "DESC"
				>
			>
		: never
	distinct?: boolean
	groupBy?: GroupByTuples<
		keyof NonNullable<RepositoryOptions<T>["queryKeys"]> & string
	>
}

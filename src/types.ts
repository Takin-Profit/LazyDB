// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { PragmaConfig } from "./pragmas.js"
import { StatementCacheOptions } from "./cache.js"
import {
	type $,
	any,
	array,
	bigint,
	bool,
	func,
	literal,
	nil,
	num,
	object,
	optional,
	partial,
	record,
	string,
	uint8Array,
	union,
	unit,
	unknown,
	validate,
} from "./utils.js"
import type { StatementSync } from "node:sqlite"

// Define the SQLite column types
const LazyDbColumnType = union([
	literal("TEXT"),
	literal("INTEGER"),
	literal("REAL"),
	literal("BOOLEAN"),
])

export type LazyDbColumnType = $<typeof LazyDbColumnType>

// Map TypeScript types to valid SQLite column types
type ValidColumnTypeMap<T> = T extends string
	? "TEXT"
	: T extends number
		? "INTEGER" | "REAL"
		: T extends boolean
			? "BOOLEAN"
			: T extends bigint
				? "INTEGER"
				: T extends null
					? LazyDbColumnType
					: never

type QueryKeyOptions<T> = {
	unique?: true
	nullable?: true
	default?: T
}

const QueryKeyOptions = partial(
	object({
		unique: literal(true),
		nullable: literal(true),
		default: any(),
	})
)

const QueryKeyDef = object({
	type: LazyDbColumnType,
	index: optional(union([literal(true), object({ unique: literal(true) })])),
	nullable: optional(literal(true)),
	default: optional(any()),
})

export type QueryKeyDef<T> = {
	type: ValidColumnTypeMap<T>
} & QueryKeyOptions<T>

export const QueryKeys = object({
	queryKeys: optional(record(string(), QueryKeyDef)),
})

export const isQueryKeyDef = (data: unknown): data is QueryKeyDef<unknown> =>
	typeof data === "object" &&
	data !== null &&
	Object.hasOwn(data, "type") &&
	typeof (data as QueryKeyDef<unknown>).type === "string"

export type QueryKeys<T> = {
	[K in keyof T]?: QueryKeyDef<T[K]>
}
export const validateQueryKeys = (data: unknown) => validate(QueryKeys, data)

export type Entity<T> = {
	_id?: number
	createdAt?: string
	updatedAt?: string
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
	queryKeys?: QueryKeys<T>
	prepareStatement: (sql: string) => StatementSync
	timestamps?: boolean
	serializer: {
		encode: (obj: unknown) => Uint8Array
		decode: (buf: Uint8Array) => unknown
	}
	logger?: (msg: string) => void
}>

export const LazyDbValue = union([
	string(),
	num(),
	bool(),
	nil(),
	bigint(),
	array(union([string(), num(), bool(), nil(), bigint()])),
])

/**
 * SQLite supported value types for WHERE conditions
 */
export type LazyDbValue = $<typeof LazyDbValue>

export const NodeSqliteValue = union([nil(), num(), bigint(), string()])

export type NodeSqliteValue = $<typeof NodeSqliteValue>

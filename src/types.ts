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
	literal("BLOB"),
	literal("BOOLEAN"),
])

export type LazyDbColumnType = $<typeof LazyDbColumnType>

const QueryKeyDef = object({
	type: LazyDbColumnType,
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

export const LazyDbValue = union([
	string(),
	num(),
	bool(),
	nil(),
	uint8Array(),
	bigint(),
	array(union([string(), num(), bool(), nil(), bigint()])),
])

/**
 * SQLite supported value types for WHERE conditions
 */
export type LazyDbValue = $<typeof LazyDbValue>

export const NodeSqliteValue = union([
	nil(),
	num(),
	bigint(),
	string(),
	uint8Array(),
])

export type NodeSqliteValue = $<typeof NodeSqliteValue>

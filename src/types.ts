// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { PragmaConfig } from "./pragmas.js"
import { StatementCacheOptions } from "./cache.js"
import {
	type $,
	any,
	bool,
	func,
	isValidationErrors,
	literal,
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

// Define the SQLite column types
const SQLiteColumnType = union([
	literal("TEXT"),
	literal("INTEGER"),
	literal("REAL"),
	literal("BLOB"),
	literal("BOOLEAN"),
])

export type SqliteColumnType = $<typeof SQLiteColumnType>

// Define the queryable column schema
const QueryColumnDef = object({
	type: SQLiteColumnType,
	unique: optional(bool()),
	primary: optional(bool()),
	nullable: optional(bool()),
	default: optional(any()),
})

export type QueryColumnDef = $<typeof QueryColumnDef>

export const QueryColumns = object({
	queryColumns: record(string(), QueryColumnDef),
})
// Make QueryKeys generic over the entity type T
export type QueryColumns<T> = {
	queryColumns: {
		[K in keyof T]?: QueryColumnDef
	}
}

export const validateQueryColumns = (data: unknown) => {
	const result = validate(QueryColumns, data)

	if (isValidationErrors(result)) {
		return result
	}
}

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

const RepositoryOptions = object({
	timestamps: optional(bool()),
})
export type RepositoryOptions = $<typeof RepositoryOptions>

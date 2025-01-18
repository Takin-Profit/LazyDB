// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { PragmaConfig } from "./pragmas.js"
import type { StatementCacheOptions } from "./cache.js"
import { validationErr, type ValidationError } from "./validate.js"

// Define the SQLite column types
export const LazyDbColumnTypes = ["TEXT", "INTEGER", "REAL", "BOOLEAN"] as const
export type LazyDbColumnType = (typeof LazyDbColumnTypes)[number]

export const SystemFieldTypes = [
	"_id",
	"__lazy_data",
	"createdAt",
	"updatedAt",
] as const
export type SystemFields = (typeof SystemFieldTypes)[number]

export type EntityType = Record<string, unknown> | object

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

export type QueryKeyDef<T = unknown> = {
	type: ValidColumnTypeMap<T>
} & QueryKeyOptions<T>

export function isQueryKeyDef(data: unknown): data is QueryKeyDef {
	if (typeof data !== "object" || data === null) {
		return false
	}
	const def = data as QueryKeyDef
	return (
		typeof def.type === "string" &&
		LazyDbColumnTypes.includes(def.type as LazyDbColumnType)
	)
}

export type DotPaths<T, Prev extends string = ""> = T extends object
	? {
			[K in keyof T & string]: T[K] extends object
				?
						| DotPaths<T[K], Prev extends "" ? K : `${Prev}.${K}`>
						| (Prev extends "" ? K : `${Prev}.${K}`)
				: Prev extends ""
					? K
					: `${Prev}.${K}`
		}[keyof T & string]
	: never

export type DotPathValue<
	T,
	Path extends string,
> = Path extends `${infer Left}.${infer Rest}`
	? Left extends keyof T
		? DotPathValue<T[Left], Rest>
		: never
	: Path extends keyof T
		? T[Path]
		: never

export type QueryKeysSchema<T> = {
	[P in Exclude<DotPaths<T>, SystemFields>]?: QueryKeyDef<DotPathValue<T, P>>
}

export type SystemQueryKeys = {
	_id?: QueryKeyDef<number>
	createdAt?: QueryKeyDef<string>
	updatedAt?: QueryKeyDef<string>
}

export type QueryKeys<T> = {
	[P in DotPaths<T>]?: QueryKeyDef<DotPathValue<T, P>>
} & SystemQueryKeys

export function validateQueryKeys(data: unknown): ValidationError[] {
	const errors: ValidationError[] = []

	if (typeof data !== "object" || data === null) {
		return [validationErr({ msg: "Query keys must be an object" })]
	}

	const queryKeys = data as Record<string, unknown>

	for (const [key, value] of Object.entries(queryKeys)) {
		if (!isQueryKeyDef(value)) {
			errors.push(
				validationErr({
					msg: `Invalid query key definition for "${key}"`,
					path: key,
				})
			)
		}
	}

	return errors
}
export type Entity<T extends EntityType> = {
	_id?: number
	createdAt?: string
	updatedAt?: string
} & T

export type SerializerConfig = {
	encode: (obj: unknown) => Uint8Array
	decode: (buf: Uint8Array) => unknown
}

export type SerializerOptions = "json" | "msgpack" | SerializerConfig

export type DatabaseOptions = {
	location: string
	timestamps?: boolean
	serializer?: SerializerOptions
	pragma?: PragmaConfig
	environment?: "development" | "testing" | "production"
	logger?: (message: string) => void
	statementCache?: true | StatementCacheOptions
}

export function validateDatabaseOptions(options: unknown): ValidationError[] {
	const errors: ValidationError[] = []

	if (typeof options !== "object" || options === null) {
		return [validationErr({ msg: "Database options must be an object" })]
	}

	const opts = options as DatabaseOptions

	if (typeof opts.location !== "string") {
		errors.push(
			validationErr({ msg: "location must be a string", path: "location" })
		)
	}

	if (opts.timestamps !== undefined && typeof opts.timestamps !== "boolean") {
		errors.push(
			validationErr({ msg: "timestamps must be a boolean", path: "timestamps" })
		)
	}

	if (
		opts.environment &&
		!["development", "testing", "production"].includes(opts.environment)
	) {
		errors.push(
			validationErr({
				msg: "environment must be 'development', 'testing', or 'production'",
				path: "environment",
			})
		)
	}

	return errors
}

export type RepositoryOptions<
	T extends EntityType,
	QK extends QueryKeysSchema<T> = QueryKeysSchema<T>,
> = Readonly<{
	queryKeys?: QK
	timestamps?: boolean
	serializer: SerializerConfig
	logger?: (msg: string) => void
}>

export function validateRepositoryOptions(
	options: unknown,
	validateSerializers = true
): ValidationError[] {
	const errors: ValidationError[] = []

	if (typeof options !== "object" || options === null) {
		return [validationErr({ msg: "Repository options must be an object" })]
	}

	const opts = options as Partial<RepositoryOptions<EntityType>>

	if (validateSerializers && !opts.serializer) {
		errors.push(
			validationErr({ msg: "serializer is required", path: "serializer" })
		)
	} else if (
		validateSerializers &&
		(typeof opts.serializer !== "object" ||
			typeof opts.serializer.encode !== "function" ||
			typeof opts.serializer.decode !== "function")
	) {
		errors.push(
			validationErr({
				msg: "serializer must have encode and decode functions",
				path: "serializer",
			})
		)
	}

	if (opts.timestamps !== undefined && typeof opts.timestamps !== "boolean") {
		errors.push(
			validationErr({ msg: "timestamps must be a boolean", path: "timestamps" })
		)
	}

	if (opts.logger !== undefined && typeof opts.logger !== "function") {
		errors.push(
			validationErr({ msg: "logger must be a function", path: "logger" })
		)
	}

	if (opts.queryKeys !== undefined) {
		const queryKeyErrors = validateQueryKeys(opts.queryKeys)
		errors.push(
			...queryKeyErrors.map((err) => ({
				...err,
				path: `queryKeys.${err.path || ""}`,
			}))
		)
	}

	return errors
}

export type LazyDbValue =
	| string
	| number
	| boolean
	| null
	| bigint
	| Array<string | number | boolean | null | bigint>
export type NodeSqliteValue = null | number | bigint | string

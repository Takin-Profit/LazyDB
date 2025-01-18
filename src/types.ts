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

export type QueryKeys<T> = QueryKeysSchema<T> & SystemQueryKeys

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
	_id: number
	createdAt?: string
	updatedAt?: string
} & T

type FirstNumber = "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
type Digit = "0" | FirstNumber

type Hours =
	| FirstNumber
	| "10"
	| "11"
	| "12"
	| "13"
	| "14"
	| "15"
	| "16"
	| "17"
	| "18"
	| "19"
	| "20"
	| "21"
	| "22"
	| "23"

export type TimeUnit = "ms" | "s" | "m" | "h" | "d"

type OneThroughFive = "1" | "2" | "3" | "4" | "5"

type MsTimeUnit =
	| `${FirstNumber}ms`
	| `${FirstNumber}${Digit}ms`
	| `${FirstNumber}${Digit}${Digit}ms`

type HoursTimeUnit = `${Hours}h`

type MinutesTimeUnit =
	| `${OneThroughFive}${Digit}m` // "10m" to "59m"
	| `60m` // "60m"
	| `${FirstNumber}m` // "1m" to "9m"

type SecondsTimeUnit =
	| `${OneThroughFive}${Digit}s` // "10s" to "59s"
	| `60s` // "60s"
	| `${FirstNumber}s` // "1s" to "9s"

type DaysTimeUnit =
	| `${FirstNumber}d`
	| `${FirstNumber}${Digit}d`
	| `${"1" | "2"}${Digit}${Digit}d`
	| `${"3"}${"0" | OneThroughFive}${Digit}d`
	| `3${"6"}${"0" | OneThroughFive}d`

export type TimeString =
	| MsTimeUnit
	| HoursTimeUnit
	| DaysTimeUnit
	| MinutesTimeUnit
	| SecondsTimeUnit

// utils/timeValidator.ts

export function isTimeString(time: string): time is TimeString {
	const msRegex = /^[1-9]\d{0,2}ms$/
	const hRegex = /^([1-9]|1\d|2[0-3])h$/
	const mRegex = /^([1-5]\d|60)m$/
	const sRegex = /^([1-5]\d|60)s$/
	const dRegex = /^([1-9]|[1-2]\d|3[0-5]\d|36[0-5])d$/

	return (
		msRegex.test(time) ||
		hRegex.test(time) ||
		mRegex.test(time) ||
		sRegex.test(time) ||
		dRegex.test(time)
	)
}

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
	logger?: (message?: string) => void
	statementCache?: true | StatementCacheOptions
	cleanupInterval?: TimeString
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

	if (opts.cleanupInterval && !isTimeString(opts.cleanupInterval)) {
		errors.push(
			validationErr({
				msg: "cleanupInterval must be a string with a time unit (ms, s, m, h, d)",
				path: "cleanupInterval",
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
	logger?: (msg?: string) => void
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

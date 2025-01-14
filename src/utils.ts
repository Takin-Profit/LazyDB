// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { type TSchema, type Static, Type } from "@sinclair/typebox"
import { AssertError, Value } from "@sinclair/typebox/value"
import type { ValueError } from "@sinclair/typebox/errors"
import type { LazyDbColumnType, LazyDbValue, NodeSqliteValue } from "./types.js"

/**
 * Capitalizes the first letter of a string and converts the rest to lowercase
 * @param str - The string to capitalize
 * @returns The capitalized string, or empty string if input is null/undefined
 */
export const capitalize = (str?: string | null): string => {
	if (!str) {
		return ""
	}
	return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase()
}

export type ValidationError = Readonly<{
	type: "validationError"
	message: string
}>

export type ValidationErrors = ValidationError[]

/**
 * Type guard to check if a value is a ValidationError
 */
export function isValidationError(value: unknown): value is ValidationError {
	return (
		typeof value === "object" &&
		value !== null &&
		"type" in value &&
		value.type === "validationError"
	)
}

/**
 * Type guard to check if a value is ValidationErrors
 */
export function isValidationErrors(value: unknown): value is ValidationErrors {
	return (
		Array.isArray(value) && !!value.length && value.every(isValidationError)
	)
}

function formatPath(path: string): string {
	return path.replace(/^\//, "").replace(/\//g, ".")
}

export function createErrorMessage(error: ValueError): string {
	const path = error.path ? formatPath(error.path) : "value"
	return `Error: ${path} invalid value '${JSON.stringify(error.value)}' - ${error.message}`
}

/**
 * Validates a value against a TypeBox schema
 * @returns The validated value or an array of validation errors
 */
export function validate<T extends TSchema>(
	schema: T,
	value: unknown
): Static<T> | ValidationErrors {
	try {
		return Value.Parse(schema, value) as Static<T>
	} catch (err) {
		if (err instanceof AssertError) {
			const errors: ValidationErrors = []

			for (const error of Value.Errors(schema, value)) {
				// Handle the main error
				errors.push({
					type: "validationError",
					message: createErrorMessage(error),
				})

				// Handle any nested errors
				if (error.errors.length > 0) {
					for (const nestedErrorIterator of error.errors) {
						for (const nestedError of nestedErrorIterator) {
							errors.push({
								type: "validationError",
								message: createErrorMessage(nestedError),
							})
						}
					}
				}
			}

			return errors
		}
		return [{ type: "validationError", message: err }]
	}
}

export const toErrMsg = (errors: ValidationErrors): string =>
	errors.map((e) => e.message).join("\n")

export const object = Type.Object
export const string = Type.String
export const num = Type.Number
export const bool = Type.Boolean
export const nil = Type.Null
export const any = Type.Any
export const literal = Type.Literal
export const union = Type.Union
export const intersect = Type.Intersect
export const tuple = Type.Tuple
export const partial = Type.Partial
export const pick = Type.Pick
export const unit = Type.Void
export const omit = Type.Omit
export const required = Type.Required
export const mapped = Type.Mapped
export const bigint = Type.BigInt
export const record = Type.Record
export const array = Type.Array
export const unknown = Type.Unknown
export const undef = Type.Undefined
export const date = Type.Date
export const eNum = Type.Enum
export const asyncIter = Type.AsyncIterator
export const uint8Array = Type.Uint8Array
export const optional = Type.Optional
export const keyOf = Type.KeyOf
export const index = Type.Index
export const extend = Type.Extends
export const template = Type.TemplateLiteral
export const promise = Type.Promise
export const constArray = (schema: Parameters<typeof Type.Array>) =>
	Type.Const(Type.Array(...schema))
// First add function type to your exports
export const func = Type.Function

// Then create the Buffer type
// Add to your existing utils.ts exports
export const buffer = () =>
	intersect([
		uint8Array(),
		object({
			byteLength: num(),
			toString: func([], string()),
			slice: func([optional(num()), optional(num())], uint8Array()),
		}),
	])

export type $<T extends TSchema> = Static<T>

export function toSqliteValue(
	value: LazyDbValue,
	columnType: LazyDbColumnType
): NodeSqliteValue {
	switch (columnType) {
		case "TEXT":
			return typeof value === "string" ? value : String(value)
		case "INTEGER":
			if (typeof value === "number" || typeof value === "bigint") {
				return value
			}
			if (typeof value === "boolean") {
				return value ? 1 : 0
			}
			throw new TypeError(`Invalid value for INTEGER: ${value}`)
		case "REAL":
			if (typeof value === "number") {
				return value
			}
			throw new TypeError(`Invalid value for REAL: ${value}`)
		case "BLOB":
			if (value instanceof Uint8Array) {
				return value
			}
			throw new TypeError(`Invalid value for BLOB: ${value}`)
		case "BOOLEAN":
			if (typeof value === "boolean") {
				return value ? 1 : 0
			}
			throw new TypeError(`Invalid value for BOOLEAN: ${value}`)
		default:
			throw new TypeError(`Unsupported SQLite column type: ${columnType}`)
	}
}

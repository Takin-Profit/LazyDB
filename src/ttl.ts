// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"
import { isTimeString } from "./types.js"
import {
	isValidationErr,
	validationErr,
	type ValidationError,
} from "./validate.js"

/**
 * Parses a time string and converts it to milliseconds.
 * Supported formats:
 * - ms: milliseconds (e.g., "500ms")
 * - s: seconds (e.g., "2s")
 * - m: minutes (e.g., "3m")
 * - h: hours (e.g., "1h")
 * - d: days (e.g., "1d")
 *
 * Examples:
 * - "500ms" => 500
 * - "2s" => 2000
 * - "3m" => 180000
 * - "1h" => 3600000
 * - "1d" => 86400000
 *
 * @param timeStr - The time string to parse.
 * @returns The equivalent time in milliseconds.
 * @throws Will throw a ValidationError if the format is invalid.
 *         Will throw a NodeSqliteError for other internal errors.
 */
export function parseTimeString(timeStr: string): number {
	const trimmedTime = timeStr.trim()

	if (!isTimeString(trimmedTime)) {
		const error: ValidationError = validationErr({
			msg: `Invalid time format: "${trimmedTime}". Supported formats are ms, s, m, h, d.`,
			path: "parseTimeString",
		})
		throw error
	}

	const regex = /^(\d+)(ms|s|m|h|d)$/i
	const match = regex.exec(trimmedTime)

	if (!match) {
		const error: ValidationError = validationErr({
			msg: `Invalid time format after validation: "${trimmedTime}".`,
			path: "parseTimeString",
		})
		throw error
	}

	const value = Number.parseInt(match[1], 10)
	const unit = match[2].toLowerCase()

	try {
		switch (unit) {
			case "ms":
				if (value < 1 || value > 999) {
					throw validationErr({
						msg: `Milliseconds value must be between 1 and 999. Received: ${value}`,
						path: "parseTimeString.ms",
					})
				}
				return value
			case "s":
				if (value < 1 || value > 60) {
					throw validationErr({
						msg: `Seconds value must be between 1 and 60. Received: ${value}`,
						path: "parseTimeString.s",
					})
				}
				return value * 1000
			case "m":
				if (value < 1 || value > 60) {
					throw validationErr({
						msg: `Minutes value must be between 1 and 60. Received: ${value}`,
						path: "parseTimeString.m",
					})
				}
				return value * 60 * 1000
			case "h":
				if (value < 1 || value > 23) {
					throw validationErr({
						msg: `Hours value must be between 1 and 23. Received: ${value}`,
						path: "parseTimeString.h",
					})
				}
				return value * 60 * 60 * 1000
			case "d":
				if (value < 1 || value > 365) {
					throw validationErr({
						msg: `Days value must be between 1 and 365. Received: ${value}`,
						path: "parseTimeString.d",
					})
				}
				return value * 24 * 60 * 60 * 1000
			default:
				// This case should never be reached due to prior validation
				throw new NodeSqliteError(
					"ERR_SQLITE_UNKNOWN_UNIT",
					SqlitePrimaryResultCode.SQLITE_ERROR,
					"Unknown time unit",
					`Unknown time unit "${unit}" encountered in parseTimeString.`
				)
		}
	} catch (error) {
		if (isValidationErr(error)) {
			// Re-throw validation errors
			throw error
		}
		// Wrap and throw as NodeSqliteError for unexpected errors
		throw new NodeSqliteError(
			"ERR_SQLITE_PARSE_TIME",
			SqlitePrimaryResultCode.SQLITE_ERROR,
			"Time Parsing Error",
			`An error occurred while parsing time string "${trimmedTime}": ${error instanceof Error ? error.message : String(error)}`,
			error instanceof Error ? error : undefined
		)
	}
}

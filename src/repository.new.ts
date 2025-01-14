// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { DatabaseSync, StatementSync } from "node:sqlite"
import { RepositoryOptions, Where } from "./types.js"

import { isValidationErrors, validate } from "./utils.js"
import {
	isNodeSqliteError,
	NodeSqliteError,
	SqlitePrimaryResultCode,
} from "./errors.js"
import type {
	WhereClauseResult,
	SupportedValue,
	FindOptions,
	QueryKeyDef,
} from "./types.js"

export function buildWhereClause<T>(where: Where<T>): WhereClauseResult {
	// Validate the where condition first
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

	// Handle single condition
	if (where.length === 3) {
		const [field, operator, value] = where

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
			const placeholders = value.map(() => "?").join(", ")
			return {
				sql: `${String(field)} ${operator} (${placeholders})`,
				params: value as SupportedValue[],
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
			}
		}

		return {
			sql: `${String(field)} ${operator} ?`,
			params: [value as SupportedValue],
		}
	}

	// Handle complex conditions
	const parts: string[] = []
	const params: SupportedValue[] = []
	let currentOperator = "AND"

	for (let i = 0; i < where.length; i++) {
		const item = where[i]

		if (i % 2 === 1) {
			// Odd indices should be operators
			currentOperator = item as string
			continue
		}

		// Even indices should be conditions
		const condition = item as Where<T>
		const { sql, params: itemParams } = buildWhereClause(condition)
		parts.push(sql)
		params.push(...itemParams)
	}

	return {
		sql:
			parts.length > 1 ? `(${parts.join(` ${currentOperator} `)})` : parts[0],
		params,
	}
}

class Repository<T extends { [key: string]: unknown }> {
	readonly #db: DatabaseSync
	readonly #prepareStatement: (sql: string) => StatementSync
	readonly #logger?: (msg: string) => void
	readonly #timestamps: boolean
	readonly #queryKeys?: RepositoryOptions<T>["queryKeys"]
	readonly #serializer: {
		encode: (obj: unknown) => Uint8Array
		decode: (buf: Uint8Array) => unknown
	}
	readonly #name: string

	constructor(
		options: RepositoryOptions<T> & {
			prepareStatement: (sql: string) => StatementSync
			db: DatabaseSync
			name: string
		}
	) {
		// Validate repository options
		const validationResult = validate(RepositoryOptions, options)
		if (isValidationErrors(validationResult)) {
			throw new NodeSqliteError(
				"ERR_SQLITE_REPOSITORY",
				SqlitePrimaryResultCode.SQLITE_MISUSE,
				"Invalid repository options",
				`Repository options validation failed: ${validationResult.map((e) => e.message).join(", ")}`,
				undefined
			)
		}

		this.#db = options.db
		this.#prepareStatement = options.prepareStatement
		this.#logger = options.logger
		this.#timestamps = options.timestamps ?? false
		this.#queryKeys = options.queryKeys
		this.#name = options.name
		this.#serializer = options.serializer

		this.#logger?.(`Repository initialized for table: ${this.#name}`)
	}

	/**
	 * Retrieves a single entity by its ID.
	 *
	 * @param {number} id The ID of the entity to retrieve
	 * @returns {T | null} The entity if found, null otherwise
	 * @throws {NodeSqliteError} If there's an error executing the query
	 */
	findById(id: number): T | null {
		this.#logger?.(`Selecting row from ${this.#name} by ID: ${id}`)

		try {
			const stmt = this.#prepareStatement(
				`SELECT * FROM ${this.#name} WHERE _id = ?`
			)

			const row = stmt.get(id) as { _id: number; data: Uint8Array } | undefined

			if (!row) {
				this.#logger?.(`No row found with ID: ${id}`)
				return null
			}

			try {
				// Attempt to deserialize the data column
				const deserializedData = this.#serializer.decode(row.data) as T

				// Combine the _id with the deserialized data
				return {
					...deserializedData,
					_id: row._id,
				} as T
			} catch (error) {
				this.#logger?.(
					`Failed to deserialize data for ID ${id}: ${error instanceof Error ? error.message : String(error)}`
				)
				throw new NodeSqliteError(
					"ERR_SQLITE_DESERIALIZE",
					SqlitePrimaryResultCode.SQLITE_MISMATCH,
					"deserialization failed",
					`Failed to deserialize data for entity with ID ${id}`,
					error instanceof Error ? error : undefined
				)
			}
		} catch (error) {
			// Handle SQLite-specific errors
			if (isNodeSqliteError(error)) {
				throw NodeSqliteError.fromNodeSqlite(error)
			}

			// Handle unexpected errors
			this.#logger?.(
				`Unexpected error getting entity by ID ${id}: ${error instanceof Error ? error.message : String(error)}`
			)
			throw new NodeSqliteError(
				"ERR_SQLITE_GET",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"get operation failed",
				`Unexpected error while getting entity with ID ${id}`,
				error instanceof Error ? error : undefined
			)
		}
	}

	find(options: FindOptions<T>): T[] {
		this.#logger?.(
			`Starting find operation with options: ${JSON.stringify(options)}`
		)

		try {
			// Start building the SQL query
			let sql = `SELECT * FROM ${this.#name}`
			const params: unknown[] = []

			// Add WHERE clause if provided
			if (options.where) {
				const whereClause = buildWhereClause(options.where)
				sql += ` WHERE ${whereClause.sql}`
				params.push(...whereClause.params)
			}

			// Add GROUP BY clause if provided
			if (options.group && options.group.length > 0) {
				sql += ` GROUP BY ${options.group.join(", ")}`
			}

			// Add ORDER BY clause if provided
			if (options.orderBy) {
				const orderClauses = Object.entries(options.orderBy).map(
					([column, direction]) => `${column} ${direction}`
				)
				if (orderClauses.length > 0) {
					sql += ` ORDER BY ${orderClauses.join(", ")}`
				}
			}

			// Add LIMIT and OFFSET if provided
			if (options.limit !== undefined) {
				sql += " LIMIT ?"
				params.push(options.limit)

				if (options.offset !== undefined) {
					sql += " OFFSET ?"
					params.push(options.offset)
				}
			}

			// Add DISTINCT if requested
			if (options.distinct) {
				sql = sql.replace("SELECT *", "SELECT DISTINCT *")
			}

			this.#logger?.(
				`Executing query: ${sql} with params: ${JSON.stringify(params)}`
			)

			const stmt = this.#prepareStatement(sql)
			const rows = stmt.all(...params) as Array<{
				_id: number
				data: Uint8Array
			}>

			// Deserialize each row
			return rows.map((row) => {
				try {
					const deserializedData = this.#serializer.decode(row.data) as T
					return {
						...deserializedData,
						_id: row._id,
					} as T
				} catch (error) {
					this.#logger?.(
						`Failed to deserialize data: ${error instanceof Error ? error.message : String(error)}`
					)
					throw new NodeSqliteError(
						"ERR_SQLITE_DESERIALIZE",
						SqlitePrimaryResultCode.SQLITE_MISMATCH,
						"deserialization failed",
						"Failed to deserialize data for entity",
						error instanceof Error ? error : undefined
					)
				}
			})
		} catch (error) {
			// Handle SQLite-specific errors
			if (isNodeSqliteError(error)) {
				throw error
			}

			// Handle unexpected errors
			this.#logger?.(
				`Unexpected error during find operation: ${error instanceof Error ? error.message : String(error)}`
			)
			throw new NodeSqliteError(
				"ERR_SQLITE_FIND",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"find operation failed",
				"Unexpected error during find operation",
				error instanceof Error ? error : undefined
			)
		}
	}
}

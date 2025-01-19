// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { DatabaseSync, StatementSync } from "node:sqlite"
import {
	type TimeString,
	validateRepositoryOptions,
	type Entity,
	type EntityType,
	type QueryKeys,
} from "./types.js"

import {
	isNodeSqliteError,
	NodeSqliteError,
	SqlitePrimaryResultCode,
} from "./errors.js"

import type stringifyLib from "fast-safe-stringify"
import { createRequire } from "node:module"
import { buildFindQuery, type FindOptions } from "./find.js"
import { buildInsertManyQuery, buildInsertQuery } from "./sql.js"
import { buildUpdateManyQuery, buildUpdateQuery } from "./update.js"
import { buildDeleteManyQuery, buildDeleteQuery } from "./delete.js"
import { isValidationErrs } from "./validate.js"
import { buildWhereClause } from "./where.js"
import { parseTimeString } from "./ttl.js"
import type { PartialDeep } from "type-fest"
const stringify: typeof stringifyLib.default = createRequire(import.meta.url)(
	"fast-safe-stringify"
).default

class Repository<T extends EntityType, QK extends QueryKeys<T> = QueryKeys<T>> {
	readonly #db: DatabaseSync
	readonly #prepareStatement: (sql: string) => StatementSync
	readonly #logger?: (msg: string) => void
	readonly #timestamps: boolean
	readonly #queryKeys?: QK
	readonly #serializer: {
		encode: (obj: unknown) => Uint8Array
		decode: (buf: Uint8Array) => unknown
	}
	readonly #name: string

	constructor(options: {
		queryKeys?: QK
		timestamps?: boolean
		serializer: {
			encode: (obj: unknown) => Uint8Array
			decode: (buf: Uint8Array) => unknown
		}
		logger?: (msg: string) => void
		prepareStatement: (sql: string) => StatementSync
		db: DatabaseSync
		name: string
	}) {
		// Validate repository options
		const validationResult = validateRepositoryOptions(options)
		if (isValidationErrs(validationResult)) {
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
	 * @returns {Entity<T> | null} The entity if found, null otherwise
	 * @throws {NodeSqliteError} If there's an error executing the query
	 */
	findById(id: number): Entity<T> | null {
		this.#logger?.(`Selecting row from ${this.#name} by ID: ${id}`)

		try {
			const stmt = this.#prepareStatement(
				`SELECT * FROM ${this.#name} WHERE _id = ?`
			)

			const row = stmt.get(id) as
				| {
						_id: number
						__lazy_data: Uint8Array
						createdAt?: string
						updatedAt?: string
				  }
				| undefined

			if (!row) {
				this.#logger?.(`No row found with ID: ${id}`)
				return null
			}

			try {
				// Attempt to deserialize the data column
				const deserializedData = this.#serializer.decode(row.__lazy_data) as T

				// Combine the _id with the deserialized data
				return {
					...deserializedData,
					_id: row._id,
					createdAt: row?.createdAt,
					updatedAt: row?.updatedAt,
				} as Entity<T>
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

	find(options: FindOptions<T, QK>): Entity<T>[] {
		this.#logger?.(
			`Starting find operation with options: ${stringify(options)}`
		)

		try {
			const { sql, params } = buildFindQuery(
				this.#name,
				options,
				this.#queryKeys
			)

			this.#logger?.(
				`Executing query: ${sql} with params: ${stringify(params)}`
			)

			const stmt = this.#prepareStatement(sql)
			const rows = stmt.all(...params) as Array<{
				_id: number
				__lazy_data: Uint8Array
				createdAt?: string
				updatedAt?: string
			}>

			// Deserialize each row
			return rows.map((row) => {
				try {
					const deserializedData = this.#serializer.decode(row.__lazy_data) as T
					return {
						...deserializedData,
						_id: row._id,
						createdAt: row?.createdAt,
						updatedAt: row?.updatedAt,
					} as Entity<T>
				} catch (error) {
					this.#logger?.(
						`Failed to deserialize data: ${
							error instanceof Error ? error.message : String(error)
						}`
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
				`Unexpected error during find operation: ${
					error instanceof Error ? error.message : String(error)
				}`
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

	findOne(options: Pick<FindOptions<T, QK>, "where">): Entity<T> | null {
		const result = this.find({ ...options, limit: 1 })
		return result.length > 0 ? result[0] : null
	}

	insert(entity: T, options?: { ttl: TimeString }): Entity<T> {
		try {
			this.#logger?.(`Inserting entity into ${this.#name}`)

			let ttl: number | undefined

			if (options?.ttl) {
				const ttlMs = parseTimeString(options.ttl)
				ttl = Date.now() + ttlMs
			}

			const serializedData = this.#serializer.encode(entity)

			// Build the insert query with query keys and timestamp handling
			const { sql, values } = buildInsertQuery(
				this.#name,
				entity,
				this.#queryKeys,
				this.#timestamps,
				ttl
			)

			this.#logger?.(`Executing query: ${sql} with values: ${values}`)

			// Prepare and execute the statement
			const stmt = this.#prepareStatement(sql)

			// Run the statement and capture the returning values
			const result = stmt.get(...values, serializedData) as {
				_id: number
				__lazy_data: Uint8Array
				createdAt?: string
				updatedAt?: string
			}

			// Deserialize the __lazy_data blob
			const deserializedData = this.#serializer.decode(result.__lazy_data) as T

			// Return the complete entity with _id and timestamps
			return {
				...deserializedData,
				_id: result._id,
				createdAt: result?.createdAt,
				updatedAt: result?.updatedAt,
			} as Entity<T>
		} catch (error) {
			// Handle SQLite-specific errors
			if (isNodeSqliteError(error)) {
				throw error
			}

			// Handle unexpected errors
			this.#logger?.(
				`Unexpected error during insert operation: ${
					error instanceof Error ? error.message : String(error)
				}`
			)
			throw new NodeSqliteError(
				"ERR_SQLITE_INSERT",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"insert operation failed",
				"Unexpected error during insert operation",
				error instanceof Error ? error : undefined
			)
		}
	}

	/**
	 * Inserts multiple entities in a single transaction.
	 *
	 * @param entities An array of entities to insert
	 * @returns An array of inserted entities with their IDs and timestamps
	 * @throws {NodeSqliteError} If the insertion fails
	 */
	insertMany(entities: T[], options?: { ttl: TimeString }): Entity<T>[] {
		if (!entities.length) {
			return []
		}

		this.#logger?.(`Inserting ${entities.length} entities into ${this.#name}`)

		try {
			// Start a transaction
			this.#db.exec("BEGIN TRANSACTION")

			let ttl: number | undefined

			if (options?.ttl) {
				const ttlMs = parseTimeString(options.ttl)
				ttl = Date.now() + ttlMs
			}

			try {
				// Build the insert query using the new helper function
				const { sql, values } = buildInsertManyQuery(
					this.#name,
					entities,
					this.#queryKeys,
					this.#timestamps,
					ttl
				)

				// Prepare the statement once for reuse
				const stmt = this.#prepareStatement(sql)

				// Process each entity
				const results: Entity<T>[] = []

				for (let i = 0; i < entities.length; i++) {
					try {
						// Serialize the entity data
						const serializedData = this.#serializer.encode(entities[i])

						// Get the values for this entity and add the serialized data
						const entityValues = [...values[i]]
						entityValues[entityValues.length - 1] = serializedData

						// Execute the statement and get the result
						const result = stmt.get(...entityValues) as {
							_id: number
							__lazy_data: Uint8Array
							createdAt?: string
							updatedAt?: string
						}

						// Deserialize and add to results
						const deserializedData = this.#serializer.decode(
							result.__lazy_data
						) as T
						results.push({
							...deserializedData,
							_id: result._id,
							createdAt: result?.createdAt,
							updatedAt: result?.updatedAt,
						} as Entity<T>)
					} catch (error) {
						this.#logger?.(
							`Failed to process entity at index ${i}: ${error instanceof Error ? error.message : String(error)}`
						)
						throw error
					}
				}

				// Commit the transaction
				this.#db.exec("COMMIT")
				this.#logger?.(`Successfully inserted ${results.length} entities`)

				return results
			} catch (error) {
				// Rollback on any error
				this.#logger?.("Rolling back transaction due to error")
				this.#db.exec("ROLLBACK")
				throw error
			}
		} catch (error) {
			// Handle SQLite-specific errors
			if (isNodeSqliteError(error)) {
				throw error
			}

			// Handle unexpected errors
			this.#logger?.(
				`Unexpected error during batch insert: ${
					error instanceof Error ? error.message : String(error)
				}`
			)
			throw new NodeSqliteError(
				"ERR_SQLITE_INSERT_MANY",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"batch insert operation failed",
				"Unexpected error during batch insert operation",
				error instanceof Error ? error : undefined
			)
		}
	}

	/**
	 * Updates entities that match the where clause with the provided partial entity data.
	 * Returns the first updated entity or null if no entities were updated.
	 *
	 * @param options Object containing where clause
	 * @param updates Partial entity containing fields to update
	 * @returns Updated entity or null if no entities were updated
	 * @throws {NodeSqliteError} If the update fails
	 */
	update(
		options: Pick<FindOptions<T, QK>, "where">,
		updates: PartialDeep<T>
	): Entity<T> | null {
		try {
			this.#db.exec("BEGIN IMMEDIATE TRANSACTION")

			try {
				// First get the existing serialized data directly
				const findSql = `SELECT _id, __lazy_data${this.#timestamps ? ", createdAt, updatedAt" : ""}
                          FROM ${this.#name}
                          WHERE ${buildWhereClause(options.where, this.#queryKeys).sql}
                          LIMIT 1`

				const findStmt = this.#prepareStatement(findSql)
				const existing = findStmt.get(
					...buildWhereClause(options.where, this.#queryKeys).params
				) as
					| {
							_id: number
							__lazy_data: Uint8Array
							createdAt?: string
							updatedAt?: string
					  }
					| undefined

				if (!existing) {
					this.#db.exec("ROLLBACK")
					return null
				}

				// Deserialize existing data and merge with updates
				const existingData = this.#serializer.decode(existing.__lazy_data) as T
				this.#logger?.(`Existing data: ${stringify(existingData)}`)
				this.#logger?.(`Updates: ${stringify(updates)}`)
				const mergedData = {
					...existingData,
					...updates,
				}

				this.#logger?.(`Merged data: ${stringify(mergedData)}`)

				// Build and execute update query
				const { sql, params, lazyDataIndex } = buildUpdateQuery(
					this.#name,
					mergedData,
					options,
					this.#queryKeys,
					this.#timestamps
				)

				const updateStmt = this.#prepareStatement(sql)
				const serializedData = this.#serializer.encode(mergedData)
				// Insert serialized data at the correct position in params
				const finalParams = [
					...params.slice(0, lazyDataIndex),
					serializedData,
					...params.slice(lazyDataIndex),
				]

				updateStmt.run(...finalParams)

				// Get the updated record
				const result = findStmt.get(
					...buildWhereClause(options.where, this.#queryKeys).params
				) as {
					_id: number
					__lazy_data: Uint8Array
					createdAt?: string
					updatedAt?: string
				}

				// Deserialize and return the result
				const deserializedData = this.#serializer.decode(
					result.__lazy_data
				) as T

				this.#db.exec("COMMIT")

				return {
					...deserializedData,
					_id: result._id,
					createdAt: result?.createdAt,
					updatedAt: result?.updatedAt,
				} as Entity<T>
			} catch (error) {
				this.#db.exec("ROLLBACK")
				throw error
			}
		} catch (error) {
			if (isNodeSqliteError(error)) {
				throw error
			}
			throw new NodeSqliteError(
				"ERR_SQLITE_UPDATE",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"update operation failed",
				"Unexpected error during update operation",
				error instanceof Error ? error : undefined
			)
		}
	}

	/**
	 * Updates multiple entities that match the where clause.
	 * For each matched entity, merges the updates with existing data and updates both
	 * queryable fields and serialized data.
	 *
	 * @param options Object containing where clause
	 * @param updates Partial entity containing fields to update
	 * @returns number Number of entities updated
	 * @throws {NodeSqliteError} If the update fails
	 */
	updateMany(
		options: Pick<FindOptions<T, QK>, "where">,
		updates: PartialDeep<T>
	): number {
		this.#logger?.(
			`Updating multiple entities in ${this.#name} with options: ${stringify(options)}`
		)

		try {
			this.#db.exec("BEGIN TRANSACTION")

			try {
				const existingEntities = this.find(options)

				if (existingEntities.length === 0) {
					this.#logger?.("No entities matched update criteria")
					this.#db.exec("COMMIT")
					return 0
				}

				const { sql, values } = buildUpdateManyQuery(
					this.#name,
					updates,
					existingEntities,
					this.#queryKeys,
					this.#timestamps
				)

				this.#logger?.(`Generated update query: ${sql}`)

				const stmt = this.#prepareStatement(sql)
				let updateCount = 0

				for (let i = 0; i < existingEntities.length; i++) {
					const existing = existingEntities[i]

					// Do a deep merge for nested structures
					const merged = this.#mergeDeep(existing, updates)

					const serializedData = this.#serializer.encode(merged)
					const entityValues = [...values[i]]

					// Find and replace the lazy data placeholder
					const lazyDataIndex = entityValues.findIndex(
						(v) => v instanceof Uint8Array
					)

					if (lazyDataIndex !== -1) {
						entityValues[lazyDataIndex] = serializedData
					}

					this.#logger?.(
						`Executing update for entity ${i + 1}/${existingEntities.length}`
					)
					this.#logger?.(`Values: ${stringify(entityValues)}`)

					const result = stmt.run(...entityValues)
					updateCount += Number(result.changes)
				}

				this.#db.exec("COMMIT")
				this.#logger?.(`Successfully updated ${updateCount} entities`)

				return updateCount
			} catch (error) {
				this.#logger?.("Rolling back transaction due to error")
				this.#db.exec("ROLLBACK")
				throw error
			}
		} catch (error) {
			if (isNodeSqliteError(error)) {
				this.#logger?.(`Caught NodeSqliteError: ${error.message}`)
				throw error
			}

			this.#logger?.(
				`Unexpected error during batch update: ${
					error instanceof Error ? error.message : String(error)
				}`
			)
			throw new NodeSqliteError(
				"ERR_SQLITE_UPDATE_MANY",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"batch update operation failed",
				"Unexpected error during batch update operation",
				error instanceof Error ? error : undefined
			)
		}
	}

	// Add this helper method to Repository class
	#mergeDeep<S>(target: S, source: PartialDeep<S, Record<string, unknown>>): S {
		const isPlainObject = (
			value: unknown
		): value is Record<string, unknown> => {
			return (
				typeof value === "object" &&
				value !== null &&
				!Array.isArray(value) &&
				Object.getPrototypeOf(value) === Object.prototype
			)
		}
		if (!isPlainObject(target) || !isPlainObject(source)) {
			return source as S
		}

		const output = { ...target }

		// Extract keys with proper type inference
		const keys = Object.keys(source) as Array<keyof typeof source>

		for (const key of keys) {
			const targetValue = output[key as keyof S]
			const sourceValue = source[key]

			if (isPlainObject(targetValue) && isPlainObject(sourceValue)) {
				// TypeScript can now infer that both values are objects
				;(output[key as keyof S] as Record<string, unknown>) = this.#mergeDeep(
					targetValue as Record<string, unknown>,
					sourceValue as PartialDeep<Record<string, unknown>>
				)
			} else if (sourceValue !== undefined) {
				output[key as keyof S] = sourceValue as (S &
					Record<string, unknown>)[keyof S]
			}
		}

		return output
	}

	/**
	 * Deletes a single entity by ID.
	 *
	 * @param id The ID of the entity to delete
	 * @returns boolean True if an entity was deleted, false if no entity was found
	 * @throws {NodeSqliteError} If the deletion fails
	 */
	deleteById(id: number): boolean {
		this.#logger?.(`Deleting entity with ID ${id} from ${this.#name}`)

		try {
			this.#db.exec("BEGIN TRANSACTION")

			try {
				const stmt = this.#prepareStatement(
					`DELETE FROM ${this.#name} WHERE _id = ?`
				)

				const result = stmt.run(id)

				// Commit transaction
				this.#db.exec("COMMIT")

				// Return true if a row was deleted, false otherwise
				return result.changes > 0
			} catch (error) {
				// Rollback on any error
				this.#logger?.("Rolling back transaction due to error")
				this.#db.exec("ROLLBACK")
				throw error
			}
		} catch (error) {
			// Handle SQLite-specific errors
			if (isNodeSqliteError(error)) {
				throw error
			}

			// Handle unexpected errors
			this.#logger?.(
				`Unexpected error during delete: ${
					error instanceof Error ? error.message : String(error)
				}`
			)
			throw new NodeSqliteError(
				"ERR_SQLITE_DELETE",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"delete operation failed",
				`Unexpected error while deleting entity with ID ${id}`,
				error instanceof Error ? error : undefined
			)
		}
	}

	/**
	 * Deletes a single entity that matches the where clause.
	 *
	 * @param options Object containing where clause
	 * @returns boolean True if an entity was deleted, false if no entity was found
	 * @throws {NodeSqliteError} If the deletion fails
	 */
	delete(options: Pick<FindOptions<T, QK>, "where">): boolean {
		try {
			const { sql, params } = buildDeleteQuery(
				this.#name,
				options,
				this.#queryKeys
			)

			const stmt = this.#prepareStatement(sql)
			const result = stmt.run(...params)

			return result.changes > 0
		} catch (error) {
			if (isNodeSqliteError(error)) {
				throw error
			}

			this.#logger?.(
				`Unexpected error during delete: ${
					error instanceof Error ? error.message : String(error)
				}`
			)
			throw new NodeSqliteError(
				"ERR_SQLITE_DELETE",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"delete operation failed",
				"Unexpected error during delete operation",
				error instanceof Error ? error : undefined
			)
		}
	}
	/**
	 * Deletes multiple entities that match the where clause.
	 *
	 * @param options Object containing where clause
	 * @returns number Number of entities deleted
	 * @throws {NodeSqliteError} If the deletion fails
	 */
	deleteMany(options: Pick<FindOptions<T, QK>, "where">): number {
		this.#logger?.(
			`Deleting multiple entities from ${this.#name} with options: ${stringify(options)}`
		)

		try {
			this.#db.exec("BEGIN TRANSACTION")

			try {
				const { sql, params } = buildDeleteManyQuery(
					this.#name,
					options,
					this.#queryKeys
				)

				const stmt = this.#prepareStatement(sql)
				const result = stmt.run(...params)

				this.#db.exec("COMMIT")

				return Number(result.changes)
			} catch (error) {
				this.#logger?.("Rolling back transaction due to error")
				this.#db.exec("ROLLBACK")
				throw error
			}
		} catch (error) {
			if (isNodeSqliteError(error)) {
				throw error
			}

			this.#logger?.(
				`Unexpected error during batch delete: ${
					error instanceof Error ? error.message : String(error)
				}`
			)
			throw new NodeSqliteError(
				"ERR_SQLITE_DELETE_MANY",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"batch delete operation failed",
				"Unexpected error during batch delete operation",
				error instanceof Error ? error : undefined
			)
		}
	}
}

export { Repository }

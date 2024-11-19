import { ABORT, type Database as LMDBDatabase, type RangeIterable } from "lmdb"
import type {
	DatabaseError,
	Document,
	Filter,
	FindOptions,
	IdGenerator,
	Result,
} from "./types.js"

/**
 * Type guard for database operation results
 */
export function isError<T>(
	result: Result<T> | undefined | null
): result is { error: DatabaseError } {
	return result != null && typeof result === "object" && "error" in result
}

/**
 * Collection class provides a MongoDB-like interface for LMDB
 *
 * Supported operations:
 * - find/findOne: Query documents with MongoDB-style filters
 * - insert/insertMany: Add new documents
 * - updateOne/updateMany: Modify existing documents
 * - removeOne/removeMany: Delete documents
 * - upsert/upsertMany: Insert or update documents
 *
 * Query operators:
 * - $eq/$ne: Equality/inequality
 * - $gt/$gte/$lt/$lte: Numeric comparisons
 * - $in/$nin: Array inclusion/exclusion
 * - $regex: Regular expression matching
 */
export class Collection<T extends Document> {
	private readonly db: LMDBDatabase<T, string>
	public readonly committed: Promise<boolean>
	public readonly flushed: Promise<boolean>
	private readonly idGenerator: IdGenerator
	private readonly logger?: (msg: string) => void

	constructor(
		db: LMDBDatabase<T, string>,
		options: { idGenerator: IdGenerator; logger?: (msg: string) => void }
	) {
		this.db = db
		this.idGenerator = options.idGenerator
		this.committed = this.db.committed
		this.flushed = this.db.flushed
		this.logger = options.logger
	}

	/**
	 * Create a standardized error object
	 */
	private createError(
		type: DatabaseError["type"],
		message: string,
		extra?: Partial<Omit<DatabaseError, "type" | "message">>
	): { error: DatabaseError } {
		return {
			error: {
				type,
				message,
				...extra,
			},
		}
	}

	/**
	 * Evaluates a single condition against a value
	 */
	private evaluateCondition(
		value: unknown,
		condition: unknown
	): Result<boolean> {
		this.logger?.(
			`evaluateCondition - Value: ${JSON.stringify(value)}, Condition: ${JSON.stringify(condition)}`
		)
		// Direct comparison for null/undefined
		if (condition === null) {
			const result = value === null
			this.logger?.(`evaluateCondition - Null comparison result: ${result}`)
			return result
		}

		// Direct value comparison for non-operator conditions
		if (
			typeof condition !== "object" ||
			condition === null ||
			Array.isArray(condition)
		) {
			return value === condition
		}

		// Operator-based comparison
		for (const [op, compareValue] of Object.entries(condition)) {
			switch (op) {
				case "$eq":
					if (value !== compareValue) return false
					break

				case "$ne":
					if (value === compareValue) return false
					break

				case "$gt":
				case "$gte":
				case "$lt":
				case "$lte": {
					// Normalize dates to timestamps for comparison
					const v = value instanceof Date ? value.getTime() : value
					const cv =
						compareValue instanceof Date ? compareValue.getTime() : compareValue

					if (typeof v !== "number" || typeof cv !== "number") {
						return this.createError(
							"VALIDATION",
							`Invalid comparison types for ${op}`,
							{
								field: String(value),
							}
						)
					}

					switch (op) {
						case "$gt":
							if (v <= cv) return false
							break
						case "$gte":
							if (v < cv) return false
							break
						case "$lt":
							if (v >= cv) return false
							break
						case "$lte":
							if (v > cv) return false
							break
					}
					break
				}

				case "$in": {
					const arr = compareValue as unknown[]
					if (!Array.isArray(arr)) {
						return this.createError(
							"VALIDATION",
							"$in requires an array value",
							{
								field: String(value),
							}
						)
					}
					if (Array.isArray(value)) {
						if (!value.some((v) => arr.includes(v))) return false
					} else {
						if (!arr.includes(value)) return false
					}
					break
				}

				case "$nin": {
					const arr = compareValue as unknown[]
					if (!Array.isArray(arr)) {
						return this.createError(
							"VALIDATION",
							"$nin requires an array value",
							{
								field: String(value),
							}
						)
					}
					if (Array.isArray(value)) {
						if (value.some((v) => arr.includes(v))) return false
					} else {
						if (arr.includes(value)) return false
					}
					break
				}

				case "$regex": {
					if (!(compareValue instanceof RegExp)) {
						return this.createError(
							"VALIDATION",
							"$regex requires a RegExp value",
							{
								field: String(value),
							}
						)
					}
					if (typeof value !== "string") {
						return this.createError(
							"VALIDATION",
							"$regex can only be applied to strings",
							{
								field: String(value),
							}
						)
					}
					if (!compareValue.test(value)) return false
					break
				}

				default:
					return this.createError("VALIDATION", `Unknown operator: ${op}`, {
						field: String(value),
					})
			}
		}

		return true
	}

	/**
	 * Evaluates a complete filter against a document
	 */
	private evaluateFilter(doc: T, filter?: Filter<T>): Result<boolean> {
		if (!filter) return true

		this.logger?.(
			`evaluateFilter - Document: ${JSON.stringify(doc)}, Filter: ${JSON.stringify(filter)}`
		)

		for (const [key, condition] of Object.entries(filter)) {
			const value = doc[key as keyof T]
			this.logger?.(
				`evaluateFilter - Checking field "${key}": ${JSON.stringify(value)}`
			)

			const result = this.evaluateCondition(value, condition)
			this.logger?.(
				`evaluateFilter - Condition result for "${key}": ${JSON.stringify(result)}`
			)

			if (isError(result)) {
				this.logger?.(
					`evaluateFilter - Error evaluating condition: ${JSON.stringify(result.error)}`
				)
				return result
			}
			if (!result) {
				this.logger?.("evaluateFilter - Condition failed")
				return false
			}
		}

		this.logger?.("evaluateFilter - All conditions passed")
		return true
	}

	/**
	 * Retrieves a single document by ID
	 */
	get(id: string): Result<T | null> {
		try {
			const value = this.db.get(id)
			return value ?? null
		} catch (error) {
			return this.createError(
				"UNKNOWN",
				`Get operation failed: ${
					error instanceof Error ? error.message : String(error)
				}`,
				{ original: error }
			)
		}
	}

	/**
	 * Checks if a document exists
	 */
	doesExist(id: string, version?: number): Result<boolean> {
		try {
			return version ? this.db.doesExist(id, version) : this.db.doesExist(id)
		} catch (error) {
			return this.createError(
				"UNKNOWN",
				`Existence check failed: ${
					error instanceof Error ? error.message : String(error)
				}`,
				{ original: error }
			)
		}
	}

	/**
	 * Prefetches documents into memory
	 */
	async prefetch(ids: string[]): Promise<Result<void>> {
		try {
			await this.db.prefetch(ids)
			return undefined
		} catch (error) {
			return this.createError(
				"UNKNOWN",
				`Prefetch operation failed: ${
					error instanceof Error ? error.message : String(error)
				}`,
				{ original: error }
			)
		}
	}

	/**
	 * Retrieves multiple documents by their IDs
	 */
	async getMany(ids: string[]): Promise<Result<(T | null)[]>> {
		try {
			const values = await this.db.getMany(ids)
			return values.map((v) => v ?? null)
		} catch (error) {
			return this.createError(
				"UNKNOWN",
				`GetMany operation failed: ${
					error instanceof Error ? error.message : String(error)
				}`,
				{ original: error }
			)
		}
	}

	/**
	 * Finds documents matching a filter
	 */
	find<R = T>(
		filter?: Filter<T>,
		options: FindOptions<T, R> = {}
	): Result<RangeIterable<R>> {
		try {
			this.logger?.(
				`Starting find operation with filter: ${JSON.stringify(filter)}`
			)

			// Start with basic range query with snapshots enabled by default
			let query = this.db.getRange({
				...options,
				snapshot: options.snapshot ?? true,
			})

			// Log the entries this query is operating on
			const allDocs = query.asArray
			this.logger?.(
				`Available documents before filtering: ${JSON.stringify(allDocs)}`
			)

			// Apply filter if provided
			if (filter) {
				query = query.filter(({ value }) => {
					const filterResult = this.evaluateFilter(value, filter)
					return filterResult
				})
			}

			// Map results
			const mappedQuery = options.map
				? (query.map(options.map) as unknown as RangeIterable<R>)
				: (query.map(({ value }) => value) as unknown as RangeIterable<R>)

			return mappedQuery.mapError((error) => {
				const errorMsg = `Find operation failed during iteration: ${
					error instanceof Error ? error.message : String(error)
				}`
				this.logger?.(errorMsg)
				throw this.createError("UNKNOWN", errorMsg, { original: error })
			})
		} catch (error) {
			const errorMsg = `Find operation failed: ${
				error instanceof Error ? error.message : String(error)
			}`
			this.logger?.(errorMsg)
			return this.createError("UNKNOWN", errorMsg, { original: error })
		}
	}

	/**
	 * Finds a single document matching a filter
	 */
	findOne(filter: Filter<T>): Result<T | null> {
		this.logger?.(
			`Starting findOne operation with filter: ${JSON.stringify(filter)}`
		)
		try {
			// Get a range over all documents with snapshot
			const range = this.db.getRange({ snapshot: true })
			for (const { value } of range) {
				const result = this.evaluateFilter(value, filter)
				if (isError(result)) {
					this.logger?.(
						`Error evaluating filter: ${JSON.stringify(result.error)}`
					)
					return result
				}
				if (result) {
					this.logger?.(`Found matching document: ${JSON.stringify(value)}`)
					return value
				}
			}
			this.logger?.("No matching document found")
			return null
		} catch (error) {
			const msg = `FindOne operation failed: ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(msg)
			return {
				error: {
					type: "UNKNOWN",
					message: msg,
					original: error,
				},
			}
		}
	}

	/**
	 * Inserts a new document
	 */
	async insert(doc: Omit<T, "_id">): Promise<Result<T>> {
		try {
			const _id = this.idGenerator()
			const document = { ...doc, _id } as T
			await this.db.put(_id, document)
			return document
		} catch (error) {
			return this.createError(
				"UNKNOWN",
				`Insert operation failed: ${
					error instanceof Error ? error.message : String(error)
				}`,
				{ original: error }
			)
		}
	}

	/**
	 * Inserts multiple documents
	 */
	insertMany(docs: Array<Omit<T, "_id">>): Promise<Result<T[]>> {
		this.logger?.("Starting insertMany operation")
		return this.transaction(() => {
			try {
				const results: T[] = []
				for (const doc of docs) {
					const _id = this.idGenerator()
					const document = { ...doc, _id } as T
					this.db.put(_id, document)
					this.logger?.(`Inserted document: ${JSON.stringify(document)}`)
					results.push(document)
				}

				// Verification reads are synchronous inside transaction
				for (const doc of results) {
					const verify = this.get(doc._id)
					this.logger?.(
						`Verification get for ${doc._id}: ${JSON.stringify(verify)}`
					)
					if (isError(verify)) return verify
					if (!verify) {
						return this.createError(
							"TRANSACTION",
							`Failed to verify insert for document ${doc._id}`
						)
					}
				}

				return results
			} catch (error) {
				const msg = `InsertMany transaction failed: ${
					error instanceof Error ? error.message : String(error)
				}`
				this.logger?.(msg)
				return this.createError("TRANSACTION", msg)
			}
		})
	}

	/**
	 * Updates an existing document or inserts a new one if it doesn't exist
	 */
	async upsert(filter: Filter<T>, doc: Omit<T, "_id">): Promise<Result<T>> {
		return await this.transaction(() => {
			try {
				const existing = this.findOne(filter)
				if (isError(existing)) return existing

				if (existing) {
					const updated = { ...existing, ...doc }
					this.db.put(existing._id, updated)
					return updated
				}

				const result = this.insert(doc)
				return result
			} catch (error) {
				return this.createError(
					"TRANSACTION",
					`Upsert operation failed: ${
						error instanceof Error ? error.message : String(error)
					}`,
					{ original: error }
				)
			}
		})
	}

	/**
	 * Updates multiple documents or inserts them if they don't exist
	 */
	upsertMany(
		operations: Array<{ filter: Filter<T>; doc: Omit<T, "_id"> }>
	): Promise<Result<T[]>> {
		return this.transaction(() => {
			try {
				const results: T[] = []

				for (const op of operations) {
					const existing = this.findOne(op.filter)
					if (isError(existing)) return existing

					if (existing) {
						// Update case
						const updated = { ...existing, ...op.doc }
						this.db.put(existing._id, updated)
						results.push(updated)
					} else {
						// Insert case
						const _id = this.idGenerator()
						const newDoc = { ...op.doc, _id } as T
						this.db.put(_id, newDoc)
						results.push(newDoc)
					}
				}

				// Verify operations
				for (const doc of results) {
					const verify = this.get(doc._id)
					if (isError(verify)) return verify
					if (!verify) {
						return this.createError(
							"TRANSACTION",
							`Failed to verify upsert for document ${doc._id}`
						)
					}
				}

				return results
			} catch (error) {
				return this.createError(
					"TRANSACTION",
					`UpsertMany operation failed: ${
						error instanceof Error ? error.message : String(error)
					}`,
					{ original: error }
				)
			}
		})
	}

	/**
	 * Updates a single document matching the filter
	 */
	async updateOne(
		filter: Filter<T>,
		update: Partial<Omit<T, "_id">>
	): Promise<Result<T | null>> {
		this.logger?.(`UpdateOne starting with filter: ${JSON.stringify(filter)}`)
		this.logger?.(`Update payload: ${JSON.stringify(update)}`)

		return this.transaction(() => {
			try {
				const existing = this.findOne(filter)
				this.logger?.(`Found document to update: ${JSON.stringify(existing)}`)

				if (isError(existing)) {
					this.logger?.(
						`Error finding document: ${JSON.stringify(existing.error)}`
					)
					return existing
				}
				if (!existing) {
					this.logger?.("No document found to update")
					return null
				}

				const updatedDoc = { ...existing, ...update }
				this.db.put(existing._id, updatedDoc)

				// Verify update
				const verify = this.get(existing._id)
				this.logger?.(`Verification read: ${JSON.stringify(verify)}`)

				if (isError(verify)) {
					return verify
				}
				if (!verify) {
					const msg = `Failed to verify update for document ${existing._id}`
					this.logger?.(msg)
					return this.createError("TRANSACTION", msg)
				}

				return verify
			} catch (error) {
				const msg = `Update failed: ${error instanceof Error ? error.message : String(error)}`
				this.logger?.(msg)
				return this.createError("UNKNOWN", msg, { original: error })
			}
		})
	}

	/**
	 * Updates all documents that match the filter
	 */
	async updateMany(
		filter: Filter<T>,
		update: Partial<Omit<T, "_id">>
	): Promise<Result<number>> {
		return await this.transaction(() => {
			try {
				const result = this.find(filter)
				if (isError(result)) return result

				let modifiedCount = 0
				for (const doc of result) {
					const updatedDoc = { ...doc, ...update }
					if (!updatedDoc._id) {
						return this.createError(
							"VALIDATION",
							"Updated document must have an _id",
							{ field: "_id" }
						)
					}
					this.db.put(updatedDoc._id, updatedDoc)
					modifiedCount++
				}

				return modifiedCount
			} catch (error) {
				return this.createError(
					"UNKNOWN",
					`Failed to update documents: ${
						error instanceof Error ? error.message : String(error)
					}`,
					{ original: error }
				)
			}
		})
	}

	/**
	 * Removes a single document that matches the filter
	 */
	async removeOne(filter: Filter<T>): Promise<Result<boolean>> {
		this.logger?.(
			`Starting removeOne operation with filter: ${JSON.stringify(filter)}`
		)
		return this.transaction(() => {
			try {
				const doc = this.findOne(filter)
				this.logger?.(`FindOne result: ${JSON.stringify(doc)}`)

				if (isError(doc)) {
					this.logger?.(`Error finding document: ${JSON.stringify(doc.error)}`)
					return doc
				}

				if (!doc) {
					this.logger?.("No document found to remove")
					return false
				}

				this.logger?.(`Attempting to remove document with id: ${doc._id}`)
				this.db.remove(doc._id)

				// Verify removal
				const verify = this.get(doc._id)
				this.logger?.(`Verification check result: ${JSON.stringify(verify)}`)

				if (verify) {
					const msg = `Failed to verify removal of document ${doc._id}`
					this.logger?.(msg)
					return this.createError("TRANSACTION", msg)
				}

				this.logger?.(`Successfully removed document ${doc._id}`)
				return true
			} catch (error) {
				const msg = `RemoveOne operation failed: ${
					error instanceof Error ? error.message : String(error)
				}`
				this.logger?.(msg)
				return this.createError("TRANSACTION", msg, { original: error })
			}
		})
	}

	/**
	 * Removes all documents that match the filter
	 */
	removeMany(filter: Filter<T>): Promise<Result<number>> {
		return this.transaction(() => {
			try {
				const result = this.find(filter)
				if (isError(result)) return result

				let removedCount = 0
				for (const doc of result) {
					if (!doc._id) {
						return this.createError(
							"VALIDATION",
							"Document missing _id field",
							{ field: "_id" }
						)
					}
					this.db.remove(doc._id)
					removedCount++
				}
				return removedCount
			} catch (error) {
				return this.createError(
					"TRANSACTION",
					`RemoveMany operation failed: ${
						error instanceof Error ? error.message : String(error)
					}`,
					{ original: error }
				)
			}
		})
	}

	/**
	 * Executes an action only if the document doesn't exist
	 */
	ifNoExists<R>(id: string, action: () => R): Promise<Result<R>> {
		try {
			return this.db
				.ifNoExists(id, () => {
					try {
						const result = action()
						if (isError(result)) {
							return ABORT // Abort transaction if action returns error
						}
						return result
					} catch (error) {
						return ABORT
					}
				})
				.then(
					(success) => {
						if (success === ABORT) {
							return this.createError(
								"CONSTRAINT",
								`Operation aborted - document ${id} already exists`,
								{ constraint: "unique_key" }
							)
						}
						return success as R
					},
					(error) =>
						this.createError(
							"TRANSACTION",
							`Conditional write failed: ${
								error instanceof Error ? error.message : String(error)
							}`,
							{ original: error }
						)
				)
		} catch (error) {
			return Promise.resolve(
				this.createError(
					"UNKNOWN",
					`ifNoExists operation failed: ${
						error instanceof Error ? error.message : String(error)
					}`,
					{ original: error }
				)
			)
		}
	}

	/**
	 * Executes a transaction
	 */
	async transaction<R>(action: () => Result<R>): Promise<Result<R>> {
		try {
			const result = await this.db.transaction(async () => {
				const actionResult = action()
				if (isError(actionResult)) {
					throw actionResult // Propagate error to trigger abort
				}
				return actionResult
			})

			return result as Result<R>
		} catch (error) {
			if (isError(error)) {
				return error as Result<R>
			}
			return this.createError(
				"TRANSACTION",
				`Transaction failed: ${
					error instanceof Error ? error.message : String(error)
				}`
			)
		}
	}
}

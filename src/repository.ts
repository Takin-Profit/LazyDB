import type { Database as LMDBDatabase, RangeIterable } from "lmdb"
import {
	TypedEventEmitter,
	type RepositoryEvents,
	type Entity,
	type FindOptions,
	type IdGenerator,
} from "./types.js"
import {
	ConstraintError,
	TransactionError,
	UnknownError,
	ValidationError,
} from "./errors.js"

/**
 * @template T The entity type stored in the repository.
 * @extends TypedEventEmitter<RepositoryEvents<T>>
 * @event entity.inserted Fired when an entity is inserted.
 * @event entities.inserted Fired when multiple entities are inserted.
 * @event entity.updated Fired when an entity is updated.
 * @event entities.updated Fired when multiple entities are updated.
 * @event entity.removed Fired when an entity is removed.
 * @event entities.removed Fired when multiple entities are removed.
 * @event entity.upserted Fired when an entity is upserted.
 * @event entities.upserted Fired when multiple entities are upserted.
 * @event repository.closed Fired when the repository is closed.
 */
export class Repository<
	T extends { [key: string]: unknown },
> extends TypedEventEmitter<
	RepositoryEvents<T> & { "repository.closed": { name: string } }
> {
	private readonly db: LMDBDatabase<T, string>
	public readonly committed: Promise<boolean>
	public readonly flushed: Promise<boolean>
	private readonly idGenerator: IdGenerator
	private readonly logger?: (msg: string) => void
	private readonly name: string
	private readonly timestamps: boolean

	constructor(
		db: LMDBDatabase<T, string>,
		options: {
			name: string
			idGenerator: IdGenerator
			timestamps?: boolean
			logger?: (msg: string) => void
		}
	) {
		super()
		this.db = db
		this.name = options.name
		this.idGenerator = options.idGenerator
		this.timestamps = options.timestamps ?? false
		this.committed = this.db.committed
		this.flushed = this.db.flushed
		this.logger = options.logger
	}

	// Helper method for timestamps
	private getTimestamps(isNew = false) {
		if (!this.timestamps) {
			return {}
		}
		const now = new Date().toISOString()
		return isNew ? { createdAt: now, updatedAt: now } : { updatedAt: now }
	}

	/**
	 * Retrieves a single entity by its ID.
	 *
	 * @param {number} id The ID of the entity to retrieve
	 * @returns {Entity | null} The entity if found, null otherwise
	 * @throws {NodeSqliteError} If there's an error executing the query
	 */
	getById(id: number): Entity | null {
		this.#logger?.(`Getting entity by ID: ${id}`)

		try {
			const stmt = this.#prepareStatement(
				`SELECT * FROM ${this.#name} WHERE _id = ?`
			)

			const row = stmt.get(id) as { _id: number; data: Uint8Array } | undefined

			if (!row) {
				this.#logger?.(`No entity found with ID: ${id}`)
				return null
			}

			// Deserialize the data column
			const deserializedData = this.#serializer.decode(row.data)

			// Combine the _id with the deserialized data
			return {
				...deserializedData,
				_id: row._id,
			} as Entity
		} catch (error) {
			this.#logger?.(
				`Error getting entity by ID ${id}: ${error instanceof Error ? error.message : String(error)}`
			)
			if (error instanceof Error) {
				throw NodeSqliteError.fromNodeSqlite(error)
			}
			throw new NodeSqliteError(
				"ERR_SQLITE_GET",
				SqlitePrimaryResultCode.SQLITE_ERROR,
				"get failed",
				`Failed to get entity with ID ${id}`,
				error instanceof Error ? error : undefined
			)
		}
	}

	/**
	 * Checks if an entity exists.
	 *
	 * @param {string} id The ID of the entity to check.
	 * @param {number} [version] Optional version to check for existence.
	 * @returns {boolean} `true` if the entity exists, otherwise `false`.
	 * @throws {UnknownError} If an unexpected error occurs during the operation.
	 */
	doesExist(id: string, version?: number): boolean {
		try {
			return version ? this.db.doesExist(id, version) : this.db.doesExist(id)
		} catch (error) {
			const errorMsg = `Existence check failed for entity ID ${id}: ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Prefetches entities into memory.
	 *
	 * @param {string[]} ids The IDs of the entities to prefetch.
	 * @returns {Promise<void>} Resolves when the prefetch operation is successful.
	 * @throws {UnknownError} If an unexpected error occurs during the operation.
	 */
	async prefetch(ids: string[]): Promise<void> {
		try {
			await this.db.prefetch(ids)
		} catch (error) {
			const errorMsg = `Prefetch operation failed for entity IDs ${JSON.stringify(ids)}: ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}
	/**
	 * Retrieves multiple entities by their IDs.
	 *
	 * @param {string[]} ids The IDs of the entities to retrieve.
	 * @returns {Promise<(T | null)[]>} Resolves to an array of entities or null for missing entries.
	 * @throws {UnknownError} If an unexpected error occurs during the operation.
	 */
	async getMany(ids: string[]): Promise<(T | null)[]> {
		try {
			const values = await this.db.getMany(ids)
			return values.map((value) => value ?? null)
		} catch (error) {
			const errorMsg = `GetMany operation failed for entity IDs ${JSON.stringify(ids)}: ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Finds entities matching the provided conditions.
	 *
	 * @param {FindOptions<T>} options Options for the query, including `where` conditions.
	 * @returns {RangeIterable<T>} A lazy iterable over the matching entities.
	 * @throws {UnknownError} If an error occurs during the find operation.
	 */
	find(options: FindOptions<T> = {}): RangeIterable<T> {
		this.logger?.(
			`Starting find operation with options: ${JSON.stringify(options)}`
		)

		try {
			// Initialize the range query with RangeOptions
			const query = this.db.getRange({
				...options,
				snapshot: options.snapshot ?? true,
			})

			// Apply `where` if provided
			if (options.where) {
				return query
					.map(({ value }) => value)
					.filter((entry) => options.where?.(entry))
			}

			// Transform the query to only return the values
			return query.map(({ value }) => value)
		} catch (error) {
			const errorMsg = `Find operation failed: ${
				error instanceof Error ? error.message : String(error)
			}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Finds a single entity matching the given condition.
	 *
	 * @param {Pick<FindOptions<T>, "where">} options Options containing the `where` condition.
	 * @returns {T | null} The matching entity, or null if no match is found.
	 * @throws {UnknownError} If an error occurs while searching for an entity.
	 */
	findOne(options: Pick<FindOptions<T>, "where">): T | null {
		this.logger?.(
			`Starting findOne operation with options: ${JSON.stringify(options)}`
		)

		try {
			// Use `find` to retrieve entities with the `where` clause
			const range = this.find({ where: options.where })

			// Iterate through the range and return the first match
			for (const value of range) {
				this.logger?.(`Found matching entity: ${JSON.stringify(value)}`)
				return value
			}

			this.logger?.("No matching entity found")
			return null
		} catch (error) {
			const errorMsg = `FindOne operation failed: ${
				error instanceof Error ? error.message : String(error)
			}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Inserts a new entity into the database.
	 *
	 * @param {Omit<T, "_id">} doc The entity to insert, excluding the `_id` field.
	 * @returns {Promise<T>} The inserted entity with the `_id` field included.
	 * @throws {ValidationError} If the entity is invalid or cannot be processed.
	 * @throws {UnknownError} If an unexpected error occurs during the insert operation.
	 * @fires Repository#entity.inserted
	 */
	async insert(doc: Omit<T, "_id" | "createdAt" | "updatedAt">): Promise<T> {
		try {
			// Generate a unique ID for the entity
			const _id = this.idGenerator()

			// Combine the new ID with the entity
			const entity = {
				...doc,
				_id,
				...this.getTimestamps(true),
			} as T

			// Attempt to insert the entity into the database
			await this.db.put(_id, entity)

			this.logger?.(`Entity inserted successfully with ID: ${_id}`)

			// Emit the "entity.inserted" event
			this.emit("entity.inserted", { entity })

			return entity
		} catch (error) {
			const errorMsg = `Insert operation failed: ${
				error instanceof Error ? error.message : String(error)
			}`
			this.logger?.(errorMsg)

			if (error instanceof TypeError || error instanceof SyntaxError) {
				// Create a ValidationError without fields since they're irrelevant here
				throw new ValidationError(errorMsg, { original: error })
			}

			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Inserts multiple entities into the database.
	 *
	 * @param {Array<Omit<T, "_id">>} docs The array of entities to insert, excluding the `_id` field.
	 * @returns {Promise<T[]>} The array of inserted entities with `_id` fields included.
	 * @throws {TransactionError} If the transaction fails or verification fails.
	 * @throws {UnknownError} If an unexpected error occurs during the insert operation.
	 * @fires Repository#entities.inserted
	 */
	async insertMany(docs: Array<Omit<T, "_id">>): Promise<T[]> {
		this.logger?.("Starting insertMany operation")

		try {
			return await this.transaction<T[]>(() => {
				const results: T[] = []

				// Insert all entities
				for (const doc of docs) {
					const _id = this.idGenerator()
					const entity = { ...doc, _id, ...this.getTimestamps(true) } as T
					this.db.put(_id, entity)
					this.logger?.(`Inserted entity: ${JSON.stringify(entity)}`)
					results.push(entity)
				}

				// Verify inserts
				for (const entity of results) {
					const verify = this.get(entity._id ?? "")

					if (!verify) {
						const errorMsg = `Failed to verify insert for entity with ID ${entity._id}`
						this.logger?.(errorMsg)
						throw new TransactionError(errorMsg)
					}
				}

				// Emit the "entities.inserted" event
				this.emit("entities.inserted", { entities: results })

				return results
			})
		} catch (error) {
			const errorMsg = `InsertMany operation failed: ${
				error instanceof Error ? error.message : String(error)
			}`
			this.logger?.(errorMsg)

			if (error instanceof TransactionError) {
				throw error
			}

			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Updates an existing entity or inserts a new one if it doesn't exist.
	 *
	 * @param {Pick<FindOptions<T>, "where">} options The options to find the existing entity.
	 * @param {Omit<T, "_id">} doc The entity to insert or update.
	 * @returns {Promise<T>} The inserted or updated entity.
	 * @throws {TransactionError} If the transaction fails.
	 * @throws {UnknownError} If an unexpected error occurs during the operation.
	 * @fires Repository#entity.upserted
	 */
	async upsert(
		options: Pick<FindOptions<T>, "where">,
		doc: Omit<T, "_id">
	): Promise<T> {
		return await this.transaction(() => {
			// Perform the query to find the existing entity
			const range = this.find({ where: options.where })
			const existing = range.asArray[0] // Only need the first matching entity

			if (existing) {
				// Update the entity
				const updated = { ...existing, ...doc, ...this.getTimestamps() }
				this.db.put(updated._id ?? "", updated)

				// Emit the "entity.upserted" event with wasInsert = false
				this.emit("entity.upserted", {
					entity: updated,
					wasInsert: false,
				})

				return updated
			}

			// Insert the entity if it doesn't exist
			const _id = this.idGenerator()
			const newEntity = { ...doc, _id } as T
			this.db.put(_id, newEntity)

			// Emit the "entity.upserted" event with wasInsert = true
			this.emit("entity.upserted", {
				entity: newEntity,
				wasInsert: true,
			})

			return newEntity
		})
	}

	/**
	 * Updates multiple entities or inserts them if they don't exist.
	 *
	 * @param {Array<{ where: FindOptions<T>["where"]; doc: Omit<T, "_id"> }>} operations The array of operations to perform.
	 * @returns {Promise<T[]>} The array of inserted or updated entities.
	 * @throws {TransactionError} If the transaction fails.
	 * @throws {UnknownError} If an unexpected error occurs during the operation.
	 * @fires Repository#entities.upserted
	 */
	async upsertMany(
		operations: Array<{ where: FindOptions<T>["where"]; doc: Omit<T, "_id"> }>
	): Promise<T[]> {
		return await this.transaction(() => {
			const results: T[] = []
			let insertCount = 0
			let updateCount = 0

			for (const op of operations) {
				// Perform a find operation to locate the existing entity
				const range = this.find({ where: op.where })
				const existing = range.asArray[0] // Only the first match is relevant

				if (existing) {
					// Update the entity
					const updated = { ...existing, ...op.doc, ...this.getTimestamps() }
					this.db.put(updated._id ?? "", updated)
					results.push(updated)
					updateCount++
				} else {
					// Insert a new entity
					const _id = this.idGenerator()
					const newEntity = { ...op.doc, _id } as T
					this.db.put(_id, newEntity)
					results.push(newEntity)
					insertCount++
				}
			}

			// Emit the "entities.upserted" event
			this.emit("entities.upserted", {
				entities: results,
				insertCount,
				updateCount,
			})

			return results
		})
	}

	/**
	 * Updates a single entity matching the filter.
	 *
	 * @param {Pick<FindOptions<T>, "where">} options The options to find the entity to update.
	 * @param {Partial<Omit<T, "_id">>} update The update payload.
	 * @returns {Promise<T | null>} The updated entity, or null if no matching entity is found.
	 * @throws {TransactionError} If the transaction fails.
	 * @throws {UnknownError} If an unexpected error occurs.
	 * @fires Repository#entity.updated
	 */
	async updateOne(
		options: Pick<FindOptions<T>, "where">,
		update: Partial<Omit<T, "_id">>
	): Promise<T | null> {
		this.logger?.(`Starting updateOne with options: ${JSON.stringify(options)}`)
		this.logger?.(`Update payload: ${JSON.stringify(update)}`)

		return await this.transaction(() => {
			const range = this.find({ where: options.where })
			const existing = range.asArray[0] // Get the first matching entity

			if (!existing) {
				this.logger?.("No entity found to update")
				return null // Indicate no update occurred
			}

			const updatedEntity = {
				...existing,
				...update,
				...this.getTimestamps(),
			}

			try {
				this.db.put(updatedEntity._id ?? "", updatedEntity)
			} catch (error) {
				throw new TransactionError(
					`Failed to update entity with ID ${updatedEntity._id}`,
					{ original: error }
				)
			}

			// Emit the "entity.updated" event
			this.emit("entity.updated", { old: existing, new: updatedEntity })

			this.logger?.(`Successfully updated entity with ID ${updatedEntity._id}`)

			return updatedEntity
		})
	}

	/**
	 * Updates all entities that match the filter.
	 *
	 * @param {Pick<FindOptions<T>, "where">} options The options to find the entities to update.
	 * @param {Partial<Omit<T, "_id">>} update The update payload.
	 * @returns {Promise<number>} The number of entities updated.
	 * @throws {TransactionError} If the transaction fails.
	 * @throws {ValidationError} If an entity is missing a required `_id` field.
	 * @throws {UnknownError} If an unexpected error occurs.
	 * @fires Repository#entities.updated
	 */
	async updateMany(
		options: Pick<FindOptions<T>, "where">,
		update: Partial<Omit<T, "_id">>
	): Promise<number> {
		this.logger?.(
			`Starting updateMany with options: ${JSON.stringify(options)}`
		)
		this.logger?.(`Update payload: ${JSON.stringify(update)}`)

		return await this.transaction(() => {
			const range = this.find({ where: options.where })
			const entities = range.asArray

			if (entities.length === 0) {
				this.logger?.("No entities found to update")
				return 0 // No updates performed
			}

			let modifiedCount = 0

			for (const entity of entities) {
				if (!entity._id) {
					throw new ValidationError("Updated entity must have an _id", {
						field: "_id",
					})
				}

				const updatedEntity = {
					...entity,
					...update,
					...this.getTimestamps(),
				}

				try {
					this.db.put(entity._id, updatedEntity)
					modifiedCount++
				} catch (error) {
					throw new TransactionError(
						`Failed to update entity with ID ${entity._id}`,
						{ original: error }
					)
				}
			}

			this.logger?.(`Updated ${modifiedCount} entities`)

			// Emit the "entities.updated" event
			this.emit("entities.updated", { count: modifiedCount })

			return modifiedCount
		})
	}

	/**
	 * Removes a single entity that matches the filter.
	 *
	 * @param {Pick<FindOptions<T>, "where">} options The options to find the entity to remove.
	 * @returns {Promise<boolean>} True if an entity was removed, otherwise false.
	 * @throws {TransactionError} If the transaction fails.
	 * @throws {UnknownError} If an unexpected error occurs.
	 * @fires Repository#entity.removed
	 */
	async removeOne(options: Pick<FindOptions<T>, "where">): Promise<boolean> {
		this.logger?.(
			`Starting removeOne operation with options: ${JSON.stringify(options)}`
		)

		return await this.transaction(() => {
			const range = this.find({ where: options.where })
			const [entity] = range.asArray

			if (!entity) {
				this.logger?.("No entity found to match the given filter.")
				return false // No entity to remove
			}

			this.logger?.(`Found entity with ID: ${entity._id}. Preparing to remove.`)

			try {
				this.db.remove(entity._id ?? "")
				this.logger?.(`Entity with ID ${entity._id} removed from the database.`)
			} catch (error) {
				const errorMsg = `Failed to remove entity with ID ${entity._id}.`
				this.logger?.(errorMsg)
				throw new TransactionError(errorMsg, { original: error })
			}

			// Verify removal
			const verify = this.get(entity._id ?? "")
			if (verify) {
				const errorMsg = `Failed to verify removal of entity with ID ${entity._id}. Entity still exists.`
				this.logger?.(errorMsg)
				throw new TransactionError(errorMsg)
			}

			this.logger?.(
				`Successfully removed and verified entity with ID: ${entity._id}`
			)

			// Emit the "entity.removed" event
			this.emit("entity.removed", { entity })

			return true
		})
	}

	/**
	 * Removes all entities that match the provided filter.
	 *
	 * @param {Pick<FindOptions<T>, "where">} options The options to find the entities to remove.
	 * @returns {Promise<number>} The number of entities removed.
	 * @throws {TransactionError} If the transaction fails.
	 * @throws {UnknownError} If an unexpected error occurs.
	 * @fires Repository#entities.removed
	 */
	async removeMany(options: Pick<FindOptions<T>, "where">): Promise<number> {
		this.logger?.(
			`Starting removeMany operation with options: ${JSON.stringify(options)}`
		)

		return await this.transaction(() => {
			const range = this.find({ where: options.where })
			const entities = range.asArray

			let removedCount = 0

			for (const entity of entities) {
				if (!entity._id) {
					throw new ValidationError("Entity is missing _id field", {
						field: "_id",
					})
				}

				try {
					this.db.remove(entity._id)
					removedCount++
				} catch (error) {
					throw new TransactionError(
						`Failed to remove entity with ID ${entity._id}`,
						{ original: error }
					)
				}
			}

			this.logger?.(`Successfully removed ${removedCount} entity(s).`)

			// Emit the "entities.removed" event
			this.emit("entities.removed", { count: removedCount })

			return removedCount
		})
	}

	/**
	 * Executes an action only if the entity with the given ID does not exist.
	 *
	 * @param {string} id The ID of the entity to check.
	 * @param {() => R} action The action to execute if the entity does not exist.
	 * @returns {Promise<R>} The result of the action if the entity does not exist.
	 * @throws {ConstraintError} If the entity already exists.
	 * @throws {TransactionError} If the conditional write fails.
	 * @throws {UnknownError} If an unexpected error occurs.
	 */
	async ifNoExists<R>(id: string, action: () => R): Promise<R> {
		try {
			const success = await this.db.ifNoExists(id, () => {
				try {
					return action()
				} catch (error) {
					this.logger?.(
						`Action execution failed: ${error instanceof Error ? error.message : String(error)}`
					)
					throw error // Re-throw the error to propagate it out of the callback
				}
			})

			if (!success) {
				throw new ConstraintError(
					`Operation aborted - entity with ID ${id} already exists`,
					{
						constraint: "unique_key",
					}
				)
			}

			return success as R
		} catch (error) {
			if (error instanceof ConstraintError) {
				throw error // Re-throw if it's a known constraint error
			}

			if (error instanceof Error) {
				this.logger?.(`Transaction failed: ${error.message}`)
				throw new TransactionError(
					`ifNoExists operation failed for entity ID ${id}`,
					{
						original: error,
					}
				)
			}

			throw new UnknownError(
				`An unknown error occurred during ifNoExists operation for entity ID ${id}`,
				{
					original: error,
				}
			)
		}
	}

	/**
	 * Executes a transaction.
	 *
	 * @param {() => R} action The action to execute within the transaction.
	 * @returns {Promise<R>} The result of the transaction.
	 * @throws {TransactionError} If the transaction fails or is aborted.
	 * @throws {UnknownError} If an unexpected error occurs during the transaction.
	 */
	async transaction<R>(action: () => R): Promise<R> {
		try {
			return await this.db.transaction(async () => {
				const actionResult = action()

				if (!actionResult) {
					throw new TransactionError("Transaction action returned no result.")
				}

				return actionResult
			})
		} catch (error) {
			if (error instanceof TransactionError) {
				this.logger?.(`Transaction failed: ${error.message}`)
				throw error
			}

			const errorMsg = `Transaction failed unexpectedly: ${
				error instanceof Error ? error.message : String(error)
			}`

			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Closes the repository and releases associated resources.
	 *
	 * @returns {Promise<void>} Resolves when the repository has been closed.
	 * @throws {UnknownError} If an unexpected error occurs during the close operation.
	 * @fires Repository#repository.closed
	 */
	async close(): Promise<void> {
		this.logger?.(`Closing repository: ${this.name}`)
		try {
			await this.db.close()
			this.emit("repository.closed", { name: this.name })
			this.logger?.(`Repository "${this.name}" closed successfully.`)
		} catch (error) {
			const errorMsg = `Failed to close repository "${this.name}": ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}
}

import { type Database as LMDBDatabase, type RootDatabase, open } from "lmdb"
import { Collection } from "./collection.js"
import type {
	Document,
	IdGenerator,
	SafeDatabaseOptions,
	SafeRootDatabaseOptionsWithPath,
} from "./types.js"
import {
	ConstraintError,
	NotFoundError,
	UnknownError,
	ValidationError,
} from "./errors.js"

export type DatabaseLogger = (msg: string) => void

export interface DatabaseConfig
	extends Partial<SafeRootDatabaseOptionsWithPath> {
	logger?: DatabaseLogger
}

export class Database {
	private readonly rootDb: RootDatabase
	private readonly collections: Map<string, Collection<Document<unknown>>>
	private readonly dbs: Map<string, LMDBDatabase>
	private readonly maxCollections: number
	private readonly idGenerator: IdGenerator
	private readonly logger?: DatabaseLogger

	constructor(path: string, options: DatabaseConfig = {}) {
		if (!path) {
			throw new ValidationError("Database path is required")
		}
		this.maxCollections = options.maxCollections ?? 12
		this.idGenerator = options.idGenerator ?? (() => crypto.randomUUID())
		this.logger = options.logger

		this.logger?.("Initializing database")
		this.rootDb = open({
			path,
			...options,
			maxDbs: this.maxCollections,
		})
		this.collections = new Map()
		this.dbs = new Map()
		this.logger?.("Database initialized")
	}

	/**
	 * Creates or retrieves a collection.
	 *
	 * @param {string} name The name of the collection.
	 * @param {Partial<SafeDatabaseOptions>} options Optional settings for the collection.
	 * @returns {Collection<Document<T>>} The created or retrieved collection.
	 * @throws {ConstraintError} If the collection already exists or the max collections limit is reached.
	 * @throws {UnknownError} If an unexpected error occurs.
	 */
	collection<T>(
		name: string,
		options?: Partial<SafeDatabaseOptions>
	): Collection<Document<T>> {
		this.logger?.(`Attempting to create/retrieve collection: ${name}`)

		if (this.collections.has(name)) {
			throw new ConstraintError(`Collection "${name}" already exists`, {
				constraint: "unique_collection",
			})
		}

		if (this.dbs.size >= this.maxCollections) {
			throw new ConstraintError(
				`Maximum number of collections (${this.maxCollections}) has been reached`,
				{ constraint: "max_collections" }
			)
		}

		try {
			if (!this.dbs.has(name)) {
				this.logger?.(`Opening new database for collection: ${name}`)
				const db = this.rootDb.openDB<Document<T>, string>(name, options || {})
				this.dbs.set(name, db)
			}

			const db = this.dbs.get(name) as LMDBDatabase<Document<T>, string>
			const collection = new Collection<Document<T>>(db, {
				idGenerator: options?.idGenerator ?? this.idGenerator,
				logger: this.logger,
			})
			this.collections.set(name, collection as Collection<Document<unknown>>)
			this.logger?.(`Collection "${name}" created successfully`)
			return collection
		} catch (error) {
			const errorMsg = `Failed to create collection "${name}": ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Clears all data from a collection.
	 *
	 * @param {string} name The name of the collection.
	 * @throws {NotFoundError} If the collection does not exist.
	 * @throws {UnknownError} If an unexpected error occurs.
	 */
	async clearCollection(name: string): Promise<void> {
		this.logger?.(`Clearing collection: ${name}`)
		const db = this.dbs.get(name)
		if (!db) {
			throw new NotFoundError(`Collection "${name}" not found`)
		}

		try {
			await db.clearAsync()
			await db.committed
			await db.flushed
			this.logger?.(`Collection "${name}" cleared`)
		} catch (error) {
			const errorMsg = `Failed to clear collection "${name}": ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Drops a collection and all its data.
	 *
	 * @param {string} name The name of the collection.
	 * @throws {NotFoundError} If the collection does not exist.
	 * @throws {UnknownError} If an unexpected error occurs.
	 */
	async dropCollection(name: string): Promise<void> {
		this.logger?.(`Dropping collection: ${name}`)
		const db = this.dbs.get(name)
		if (!db) {
			throw new NotFoundError(`Collection "${name}" not found`)
		}

		try {
			await db.drop()
			await db.committed
			await db.flushed
			this.dbs.delete(name)
			this.collections.delete(name)
			this.logger?.(`Collection "${name}" dropped`)
		} catch (error) {
			const errorMsg = `Failed to drop collection "${name}": ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Clears all collections.
	 *
	 * @throws {UnknownError} If an unexpected error occurs.
	 */
	async clearAll(): Promise<void> {
		this.logger?.("Clearing all collections")
		try {
			await Promise.all(
				Array.from(this.dbs.keys()).map((name) => this.clearCollection(name))
			)
			this.logger?.("All collections cleared")
		} catch (error) {
			const errorMsg = `Failed to clear all collections: ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Closes the database and all collections.
	 *
	 * @throws {UnknownError} If an unexpected error occurs.
	 */
	async close(): Promise<void> {
		this.logger?.("Closing database")
		try {
			await Promise.all(
				Array.from(this.collections.values()).map(async (collection) => {
					await collection.committed
					await collection.flushed
				})
			)
			await this.rootDb.close()
			this.collections.clear()
			this.dbs.clear()
			this.logger?.("Database closed")
		} catch (error) {
			const errorMsg = `Failed to close database: ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}
	/**
	 * Creates a snapshot backup of the database at the specified path.
	 *
	 * @param {string} path The path where the backup will be created.
	 * @param {boolean} [compact=false] Whether to apply compaction while making the backup (slower but smaller).
	 * @throws {ValidationError} If the backup path is invalid or not provided.
	 * @throws {UnknownError} If an unexpected error occurs during the backup operation.
	 */
	async backup(path: string, compact = false): Promise<void> {
		if (!path) {
			throw new ValidationError("Backup path is required")
		}

		this.logger?.(`Starting backup to path: ${path} with compact: ${compact}`)

		try {
			await this.rootDb.backup(path, compact)
			this.logger?.(`Backup completed successfully to path: ${path}`)
		} catch (error) {
			const errorMsg = `Backup operation failed: ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}
}

import { type Database as LMDBDatabase, type RootDatabase, open } from "lmdb"
import { Collection } from "./collection.js"
import type {
	Document,
	IdGenerator,
	Result,
	SafeDatabaseOptions,
	SafeRootDatabaseOptionsWithPath,
} from "./types.js"

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
			throw new Error("Database path is required")
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
	 * Creates or retrieves a collection
	 */
	collection<T>(
		name: string,
		options?: Partial<SafeDatabaseOptions>
	): Result<Collection<Document<T>>> {
		this.logger?.(`Attempting to create/retrieve collection: ${name}`)

		if (this.collections.has(name)) {
			this.logger?.(`Collection "${name}" already exists`)
			return {
				error: {
					type: "CONSTRAINT",
					message: `Collection "${name}" already exists`,
					constraint: "unique_collection",
				},
			}
		}

		if (this.dbs.size >= this.maxCollections) {
			this.logger?.(
				`Maximum number of collections (${this.maxCollections}) reached`
			)
			return {
				error: {
					type: "CONSTRAINT",
					message: `Maximum number of collections (${this.maxCollections}) has been reached`,
					constraint: "max_collections",
				},
			}
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
			const errorMsg = `Failed to create collection "${name}": ${
				error instanceof Error ? error.message : String(error)
			}`
			this.logger?.(errorMsg)
			return {
				error: {
					type: "UNKNOWN",
					message: errorMsg,
					original: error,
				},
			}
		}
	}

	/**
	 * Clears all data from a collection
	 */
	async clearCollection(name: string): Promise<void> {
		this.logger?.(`Clearing collection: ${name}`)
		const db = this.dbs.get(name)
		if (!db) {
			const errorMsg = `Collection "${name}" not found`
			this.logger?.(errorMsg)
			throw new Error(errorMsg)
		}

		await db.clearAsync()
		await db.committed
		await db.flushed
		this.logger?.(`Collection "${name}" cleared`)
	}

	/**
	 * Drops a collection and all its data
	 */
	async dropCollection(name: string): Promise<void> {
		this.logger?.(`Dropping collection: ${name}`)
		const db = this.dbs.get(name)
		if (!db) {
			const errorMsg = `Collection "${name}" not found`
			this.logger?.(errorMsg)
			throw new Error(errorMsg)
		}

		await db.drop()
		await db.committed
		await db.flushed
		this.dbs.delete(name)
		this.collections.delete(name)
		this.logger?.(`Collection "${name}" dropped`)
	}

	/**
	 * Clears all collections
	 */
	async clearAll(): Promise<void> {
		this.logger?.("Clearing all collections")
		await Promise.all(
			Array.from(this.dbs.keys()).map((name) => this.clearCollection(name))
		)
		this.logger?.("All collections cleared")
	}

	/**
	 * Closes the database and all collections
	 */
	async close(): Promise<void> {
		this.logger?.("Closing database")
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
	}
}

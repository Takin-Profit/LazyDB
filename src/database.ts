import { type Database as LMDBDatabase, type RootDatabase, open } from "lmdb"
import { Repository } from "./repository.js"
import type {
	Entity,
	IdGenerator,
	SafeDatabaseOptions,
	SafeRootDatabaseOptionsWithPath,
	DatabaseEvents,
} from "./types.js"
import {
	ConstraintError,
	NotFoundError,
	UnknownError,
	ValidationError,
} from "./errors.js"
import { TypedEventEmitter } from "./types.js"

export type DatabaseLogger = (msg: string) => void

export interface DatabaseConfig
	extends Partial<SafeRootDatabaseOptionsWithPath> {
	logger?: DatabaseLogger
}

/**
 * Database class provides a high-level interface for managing LMDB repositories.
 *
 * @event repository.created Fired when a new repository is created
 * @event repository.cleared Fired when a repository is cleared
 * @event repository.dropped Fired when a repository is dropped
 * @event database.cleared Fired when all repositories are cleared
 * @event database.closed Fired when the database is closed
 * @event backup.started Fired when a backup operation begins
 * @event backup.completed Fired when a backup operation completes successfully
 * @event backup.failed Fired when a backup operation fails
 */
export class Database extends TypedEventEmitter<DatabaseEvents> {
	private readonly rootDb: RootDatabase
	private readonly repositories: Map<string, Repository<Entity<unknown>>>
	private readonly dbs: Map<string, LMDBDatabase>
	private readonly maxRepositories: number
	private readonly idGenerator: IdGenerator
	private readonly logger?: DatabaseLogger
	private readonly timestampEnabled: boolean

	/**
	 * Gets the root database instance.
	 */
	get rootDB(): RootDatabase {
		return this.rootDb
	}

	/**
	 * Creates a new Database instance.
	 *
	 * @param {string} path The file system path where the database will be stored
	 * @param {DatabaseConfig} options Configuration options for the database
	 * @throws {ValidationError} If the database path is not provided
	 */
	constructor(
		path: string,
		options: DatabaseConfig & { timestamps?: boolean } = {}
	) {
		super()
		if (!path) {
			throw new ValidationError("Database path is required")
		}
		this.maxRepositories = options.maxRepositories ?? 12
		this.idGenerator = options.idGenerator ?? (() => crypto.randomUUID())
		this.logger = options.logger
		this.timestampEnabled = options.timestamps ?? false

		this.logger?.("Initializing database")
		this.rootDb = open({
			path,
			...options,
			maxDbs: this.maxRepositories,
		})
		this.repositories = new Map()
		this.dbs = new Map()
		this.logger?.("Database initialized")
	}

	/**
	 * Creates or retrieves a repository.
	 *
	 * @param {string} name The name of the repository.
	 * @param {Partial<SafeDatabaseOptions>} options Optional settings for the repository.
	 * @returns {Repository<Entity<T>>} The created or retrieved repository.
	 * @throws {ConstraintError} If the max repositories limit is reached.
	 * @throws {UnknownError} If an unexpected error occurs.
	 * @fires Database#repository.created - Only fired when a new repository is created
	 */
	repository<T>(
		name: string,
		options?: Partial<SafeDatabaseOptions & { timestamps?: boolean }>
	): Repository<Entity<T>> {
		this.logger?.(`Attempting to create/retrieve repository: ${name}`)

		// Return existing repository if it exists
		if (this.repositories.has(name)) {
			this.logger?.(`Returning existing repository: ${name}`)
			return this.repositories.get(name) as Repository<Entity<T>>
		}

		if (this.dbs.size >= this.maxRepositories) {
			throw new ConstraintError(
				`Maximum number of repositories (${this.maxRepositories}) has been reached`,
				{ constraint: "max_repositories" }
			)
		}

		try {
			if (!this.dbs.has(name)) {
				this.logger?.(`Opening new database for repository: ${name}`)
				const db = this.rootDb.openDB<Entity<T>, string>(name, options || {})
				this.dbs.set(name, db)
			}

			const db = this.dbs.get(name) as LMDBDatabase<Entity<T>, string>
			const repository = new Repository<Entity<T>>(db, {
				name,
				idGenerator: options?.idGenerator ?? this.idGenerator,
				logger: this.logger,
				timestamps: options?.timestamps ?? this.timestampEnabled,
			})
			this.repositories.set(name, repository as Repository<Entity<unknown>>)
			this.emit("repository.created", { name })
			this.logger?.(`Repository "${name}" created successfully`)
			return repository
		} catch (error) {
			const errorMsg = `Failed to create repository "${name}": ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Clears all data from a repository.
	 *
	 * @param {string} name The name of the repository.
	 * @throws {NotFoundError} If the repository does not exist.
	 * @throws {UnknownError} If an unexpected error occurs.
	 * @fires Database#repository.cleared
	 */
	async clearRepository(name: string): Promise<void> {
		this.logger?.(`Clearing repository: ${name}`)
		const db = this.dbs.get(name)
		if (!db) {
			throw new NotFoundError(`Repository "${name}" not found`)
		}

		try {
			await db.clearAsync()
			await db.committed
			await db.flushed
			this.emit("repository.cleared", { name })
			this.logger?.(`Repository "${name}" cleared`)
		} catch (error) {
			const errorMsg = `Failed to clear repository "${name}": ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Drops a repository and all its data.
	 *
	 * @param {string} name The name of the repository.
	 * @throws {NotFoundError} If the repository does not exist.
	 * @throws {UnknownError} If an unexpected error occurs.
	 * @fires Database#repository.dropped
	 */
	async dropRepository(name: string): Promise<void> {
		this.logger?.(`Dropping repository: ${name}`)
		const db = this.dbs.get(name)
		if (!db) {
			throw new NotFoundError(`Repository "${name}" not found`)
		}

		try {
			await db.drop()
			await db.committed
			await db.flushed
			this.dbs.delete(name)
			this.repositories.delete(name)
			this.emit("repository.dropped", { name })
			this.logger?.(`Repository "${name}" dropped`)
		} catch (error) {
			const errorMsg = `Failed to drop repository "${name}": ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Clears all repositories.
	 *
	 * @throws {UnknownError} If an unexpected error occurs.
	 * @fires Database#database.cleared
	 */
	async clearAll(): Promise<void> {
		this.logger?.("Clearing all repositories")
		try {
			await Promise.all(
				Array.from(this.dbs.keys()).map((name) => this.clearRepository(name))
			)
			this.emit("database.cleared", null)
			this.logger?.("All repositories cleared")
		} catch (error) {
			const errorMsg = `Failed to clear all repositories: ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Closes the database and all repositories.
	 *
	 * @throws {UnknownError} If an unexpected error occurs.
	 * @fires Database#database.closed
	 */
	async close(): Promise<void> {
		this.logger?.("Closing database")
		try {
			await Promise.all(
				Array.from(this.repositories.values()).map(async (repository) => {
					await repository.committed
					await repository.flushed
				})
			)
			await this.rootDb.close()
			this.repositories.clear()
			this.dbs.clear()
			this.emit("database.closed", null)
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
	 * @fires Database#backup.started
	 * @fires Database#backup.completed
	 * @fires Database#backup.failed
	 */
	async backup(path: string, compact = false): Promise<void> {
		if (!path) {
			throw new ValidationError("Backup path is required")
		}

		this.logger?.(`Starting backup to path: ${path} with compact: ${compact}`)
		this.emit("backup.started", { path, compact })

		try {
			await this.rootDb.backup(path, compact)
			this.emit("backup.completed", { path, compact })
			this.logger?.(`Backup completed successfully to path: ${path}`)
		} catch (error) {
			const errorMsg = `Backup operation failed: ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			this.emit("backup.failed", { path, error: error as Error })
			throw new UnknownError(errorMsg, { original: error })
		}
	}

	/**
	 * Closes a specific repository.
	 *
	 * @param {string} name The name of the repository to close.
	 * @returns {Promise<void>} Resolves when the repository is successfully closed.
	 * @throws {NotFoundError} If the repository does not exist.
	 * @throws {UnknownError} If an unexpected error occurs during the operation.
	 * @fires Database#repository.closed
	 */
	async closeRepository(name: string): Promise<void> {
		this.logger?.(`Closing repository: ${name}`)
		const db = this.dbs.get(name)
		if (!db) {
			throw new NotFoundError(`Repository "${name}" not found`)
		}

		try {
			await db.committed
			await db.flushed
			this.dbs.delete(name)
			this.repositories.delete(name)
			this.emit("repository.closed", { name })
			this.logger?.(`Repository "${name}" successfully closed.`)
		} catch (error) {
			const errorMsg = `Failed to close repository "${name}": ${error instanceof Error ? error.message : String(error)}`
			this.logger?.(errorMsg)
			throw new UnknownError(errorMsg, { original: error })
		}
	}
}

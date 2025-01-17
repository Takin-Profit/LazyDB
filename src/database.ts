import { DatabaseSync, type StatementSync } from "node:sqlite"
import msgpackLite from "msgpack-lite"
import { createRequire } from "node:module"
import {
	type PragmaConfig,
	PragmaDefaults,
	getPragmaStatements,
} from "./pragmas.js"
import {
	NodeSqliteError,
	isNodeSqliteError,
	SqlitePrimaryResultCode,
} from "./errors.js"
import {
	type RepositoryOptions,
	type SerializerOptions,
	type DatabaseOptions,
	type QueryKeysSchema,
	type EntityType,
	type SystemQueryKeys,
	validateRepositoryOptions,
	validateDatabaseOptions,
	type TimeString,
} from "./types.js"
import { Repository } from "./repository.js"
import {
	createStatementCache,
	type StatementCache,
	type CacheStats,
} from "./cache.js"
import type stringifyLib from "fast-safe-stringify"
import { buildCreateTableSQL, createIndexes } from "./sql.js"
import { isValidationErrs } from "./validate.js"
import { parseTimeString } from "./ttl.js"
import { join } from "node:path"
import { tmpdir } from "node:os"
import { accessSync, renameSync, unlinkSync } from "node:fs"
const stringify: typeof stringifyLib.default = createRequire(import.meta.url)(
	"fast-safe-stringify"
).default

type RepositoryFactory<T extends EntityType> = {
	create<K extends QueryKeysSchema<T>>(
		options?: Omit<RepositoryOptions<T, QueryKeysSchema<T>>, "serializer"> & {
			queryKeys?: QueryKeysSchema<T> // Force literal type checking
		}
	): Repository<T, K & SystemQueryKeys>
}

type CreateFactoryProps = {
	db: DatabaseSync
	logger?: (message: string) => void
	name: string
	prepareStatement: (sql: string) => StatementSync
	serializer: {
		encode: (obj: unknown) => Uint8Array
		decode: (buf: Uint8Array) => unknown
	}
	timestampEnabled?: boolean
}

function _createIndexes<T extends EntityType>(
	name: string,
	db: DatabaseSync,
	queryKeys: QueryKeysSchema<T>,
	logger?: (message: string) => void
): void {
	const statements = createIndexes(name, queryKeys)
	for (const sql of statements) {
		try {
			db.exec(sql)
		} catch (error) {
			logger?.(
				`Failed to create index: ${error instanceof Error ? error.message : String(error)}`
			)
			throw error instanceof NodeSqliteError
				? error
				: NodeSqliteError.fromNodeSqlite(
						error instanceof Error ? error : new Error(String(error))
					)
		}
	}
}

const createRepositoryFactory = <T extends EntityType>(
	props: CreateFactoryProps
): RepositoryFactory<T> => ({
	create<K extends QueryKeysSchema<T>>(
		options?: Omit<RepositoryOptions<T, QueryKeysSchema<T>>, "serializer"> & {
			queryKeys?: QueryKeysSchema<T> // Force literal type checking
		}
	) {
		const timestampsEnabled = props.timestampEnabled ?? options?.timestamps
		props.logger?.(
			`Creating repository: ${props.name}, timestamps enabled: ${timestampsEnabled}`
		)

		const result = validateRepositoryOptions(options, false)

		if (isValidationErrs(result)) {
			throw new NodeSqliteError(
				"ERR_SQLITE_REPOSITORY",
				SqlitePrimaryResultCode.SQLITE_MISUSE,
				"Invalid repository options",
				`Repository options validation failed: ${result.map((e) => e.message).join(", ")}`,
				undefined
			)
		}

		if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(props.name)) {
			// Validate repository name
			throw new NodeSqliteError(
				"ERR_SQLITE_REPOSITORY",
				SqlitePrimaryResultCode.SQLITE_MISUSE,
				"Invalid repository name",
				`Repository name "${props.name}" must start with a letter or underscore and contain only alphanumeric characters and underscores`,
				undefined
			)
		}

		let extendedQueryKeys: QueryKeysSchema<T> = options?.queryKeys ?? {}

		if (options?.queryKeys) {
			const systemFields: string[] = [
				"_id",
				"__lazy_data",
				"createdAt",
				"updatedAt",
			]

			const invalidFields = Object.keys(options.queryKeys).filter((key) =>
				systemFields.includes(key)
			)

			if (invalidFields.length > 0) {
				throw new NodeSqliteError(
					"ERR_SQLITE_REPOSITORY",
					SqlitePrimaryResultCode.SQLITE_MISUSE,
					"Invalid query keys",
					`System fields cannot be used as query keys: ${invalidFields.join(", ")}`,
					undefined
				)
			}

			const systemQueryKeys: SystemQueryKeys = {
				_id: { type: "INTEGER" },
				...(timestampsEnabled
					? {
							createdAt: { type: "TEXT" },
							updatedAt: { type: "TEXT" },
						}
					: {}),
			}

			extendedQueryKeys = {
				...options?.queryKeys,
				...systemQueryKeys,
			} as K & SystemQueryKeys
		}

		// Build CREATE TABLE statement
		const createTableSQL = buildCreateTableSQL(
			props.name,
			extendedQueryKeys,
			timestampsEnabled
		)

		try {
			// Create table if not exists
			props.db.exec(createTableSQL)

			// Create indexes for queryable columns
			if (options?.queryKeys) {
				_createIndexes(props.name, props.db, extendedQueryKeys, props.logger)
			}

			// Return new Repository instance
			return new Repository<T, K & SystemQueryKeys>({
				prepareStatement: props.prepareStatement,
				serializer: props.serializer,
				timestamps: timestampsEnabled,
				queryKeys: extendedQueryKeys as K & SystemQueryKeys,
				logger: options?.logger ?? props.logger,
				name: props.name,
				db: props.db,
			})
		} catch (error) {
			props.logger?.(
				`Failed to create repository: ${error instanceof Error ? error.message : String(error)}`
			)
			throw error instanceof NodeSqliteError
				? error
				: NodeSqliteError.fromNodeSqlite(
						error instanceof Error ? error : new Error(String(error))
					)
		}
	},
})

class LazyDb {
	readonly #logger?: (message?: string) => void
	readonly #timestampEnabled: boolean
	readonly #textDecoder = new TextDecoder()
	readonly #location: string
	#db: DatabaseSync

	#intervalId?: NodeJS.Timeout

	readonly #serializer: {
		encode: (obj: unknown) => Uint8Array
		decode: (buf: Uint8Array) => unknown
	}
	readonly #statementCache: StatementCache | undefined

	constructor(options: DatabaseOptions = { location: ":memory:" }) {
		try {
			this.#location = options.location
			this.#logger = options.logger
			this.#logger?.(`Opening database at ${options.location}`)

			// Validate options
			const validationResult = validateDatabaseOptions(options)
			if (isValidationErrs(validationResult)) {
				this.#logger?.("Configuration validation failed")
				throw new NodeSqliteError(
					"ERR_SQLITE_CONFIG",
					SqlitePrimaryResultCode.SQLITE_MISUSE,
					"Invalid database configuration",
					`Configuration validation failed: ${validationResult.map((e) => e.message).join(", ")}`,
					undefined
				)
			}

			// Initialize database with proper error handling
			this.#db = new DatabaseSync(options.location, { open: true })
			this.#timestampEnabled = options.timestamps ?? true

			this.#logger?.("Database opened successfully")

			// Setup serializer
			this.#logger?.(
				`Initializing serializer (${options.serializer ?? "msgpack"})`
			)
			this.#serializer = this.#initializeSerializer(
				options.serializer ?? "msgpack"
			)
			this.#logger?.("Serializer initialized")

			// Initialize statement cache
			if (options.statementCache === true) {
				this.#logger?.("Initializing statement cache with default options")
				this.#statementCache = createStatementCache({
					maxSize: 1000,
				})
			} else if (options.statementCache) {
				this.#logger?.("Initializing statement cache with custom options")
				this.#statementCache = createStatementCache(options.statementCache)
			} else {
				this.#logger?.("Statement cache disabled")
			}

			// Apply pragmas based on environment and custom settings
			const environment = options.environment || "development"
			this.#logger?.(`Configuring pragmas for ${environment} environment`)
			const defaultPragmas = PragmaDefaults[environment]
			const customPragmas = options.pragma || {}

			// Merge default and custom pragmas
			const finalPragmas: PragmaConfig = {
				...defaultPragmas,
				...customPragmas,
			}

			// Configure pragmas with error handling
			this.#configurePragmas(finalPragmas)

			if (options.cleanupInterval) {
				this.#startCleanupInterval(options.cleanupInterval)
			}

			this.#logger?.("Database initialization complete")
		} catch (error) {
			this.#logger?.(
				`Database initialization failed: ${error instanceof Error ? error.message : String(error)}`
			)
			if (isNodeSqliteError(error)) {
				throw error
			}
			throw NodeSqliteError.fromNodeSqlite(
				error instanceof Error ? error : new Error(String(error))
			)
		}
	}

	repository<T extends EntityType>(name: string): RepositoryFactory<T> {
		return createRepositoryFactory<T>({
			db: this.#db,
			logger: this.#logger,
			name,
			prepareStatement: this.prepareStatement.bind(this),
			serializer: this.#serializer,
			timestampEnabled: this.#timestampEnabled ?? true,
		})
	}

	prepareStatement(sql: string): StatementSync {
		try {
			// If cache is enabled, check for cached statement
			if (this.#statementCache) {
				const cached = this.#statementCache.get(sql)
				if (cached) {
					this.#logger?.("Statement cache hit")
					return cached
				}
				this.#logger?.("Statement cache miss")
			}

			// Prepare new statement
			this.#logger?.("Preparing new SQL statement")
			const stmt = this.#db.prepare(sql)

			// Cache if caching is enabled
			if (this.#statementCache) {
				this.#logger?.("Caching prepared statement")
				this.#statementCache.set(sql, stmt)
			}

			return stmt
		} catch (error) {
			this.#logger?.(
				`Failed to prepare statement: ${error instanceof Error ? error.message : String(error)}`
			)
			if (isNodeSqliteError(error)) {
				if (
					error.getPrimaryResultCode() === SqlitePrimaryResultCode.SQLITE_NOMEM
				) {
					if (this.#statementCache) {
						this.#logger?.("Memory pressure detected, clearing statement cache")
						this.#statementCache.clear()
					}
					throw new NodeSqliteError(
						"ERR_SQLITE_OOM",
						SqlitePrimaryResultCode.SQLITE_NOMEM,
						"out of memory",
						"Failed to prepare statement due to memory constraints. Cache has been cleared.",
						error
					)
				}
				if (
					error.getPrimaryResultCode() === SqlitePrimaryResultCode.SQLITE_ERROR
				) {
					throw new NodeSqliteError(
						"ERR_SQLITE_SYNTAX",
						SqlitePrimaryResultCode.SQLITE_ERROR,
						"syntax error",
						`SQL syntax error in statement: ${sql}`,
						error
					)
				}
				throw error
			}
			throw NodeSqliteError.fromNodeSqlite(
				error instanceof Error ? error : new Error(String(error))
			)
		}
	}

	#configurePragmas(config: PragmaConfig): void {
		try {
			this.#logger?.("Applying pragma configuration")
			const statements = getPragmaStatements(config)
			for (const stmt of statements) {
				this.#logger?.(`Executing pragma: ${stmt}`)
				this.#db.exec(stmt)
			}
			this.#logger?.("Pragma configuration complete")
		} catch (error) {
			this.#logger?.(
				`Failed to configure pragmas: ${error instanceof Error ? error.message : String(error)}`
			)
			if (isNodeSqliteError(error)) {
				if (
					error.getPrimaryResultCode() === SqlitePrimaryResultCode.SQLITE_BUSY
				) {
					throw new NodeSqliteError(
						"ERR_SQLITE_BUSY",
						SqlitePrimaryResultCode.SQLITE_BUSY,
						"Database is locked while configuring pragmas",
						"Failed to configure database pragmas: database is locked",
						error
					)
				}
				throw error
			}
			throw NodeSqliteError.fromNodeSqlite(
				error instanceof Error ? error : new Error(String(error))
			)
		}
	}

	#initializeSerializer(serializerOption: SerializerOptions): {
		encode: (obj: unknown) => Uint8Array
		decode: (buf: Uint8Array) => unknown
	} {
		try {
			// Validate serializer options
			this.#logger?.("Validating serializer configuration")

			if (typeof serializerOption === "object") {
				this.#logger?.("Using custom serializer")
				return serializerOption
			}

			if (serializerOption === "json") {
				this.#logger?.("Using JSON serializer")
				return {
					encode: (obj: unknown) => {
						try {
							return new Uint8Array(Buffer.from(stringify(obj)))
						} catch (error) {
							this.#logger?.(
								`JSON serialization failed: ${error instanceof Error ? error.message : String(error)}`
							)
							throw new NodeSqliteError(
								"ERR_SQLITE_SERIALIZE",
								SqlitePrimaryResultCode.SQLITE_ERROR,
								"JSON serialization failed",
								`Failed to serialize object: ${error instanceof Error ? error.message : String(error)}`,
								error instanceof Error ? error : undefined
							)
						}
					},
					decode: (buf: Uint8Array) => {
						try {
							return JSON.parse(this.#textDecoder.decode(buf))
						} catch (error) {
							this.#logger?.(
								`JSON deserialization failed: ${error instanceof Error ? error.message : String(error)}`
							)
							throw new NodeSqliteError(
								"ERR_SQLITE_DESERIALIZE",
								SqlitePrimaryResultCode.SQLITE_ERROR,
								"JSON deserialization failed",
								`Failed to deserialize data: ${error instanceof Error ? error.message : String(error)}`,
								error instanceof Error ? error : undefined
							)
						}
					},
				}
			}

			// msgpack-lite serializer with error handling
			this.#logger?.("Using MessagePack serializer")
			return {
				encode: (obj: unknown) => {
					try {
						return new Uint8Array(msgpackLite.encode(obj))
					} catch (error) {
						this.#logger?.(
							`MessagePack serialization failed: ${error instanceof Error ? error.message : String(error)}`
						)
						throw new NodeSqliteError(
							"ERR_SQLITE_SERIALIZE",
							SqlitePrimaryResultCode.SQLITE_ERROR,
							"MessagePack serialization failed",
							`Failed to serialize object: ${error instanceof Error ? error.message : String(error)}`,
							error instanceof Error ? error : undefined
						)
					}
				},
				decode: (buf: Uint8Array) => {
					try {
						return msgpackLite.decode(new Uint8Array(buf))
					} catch (error) {
						this.#logger?.(
							`MessagePack deserialization failed: ${error instanceof Error ? error.message : String(error)}`
						)
						throw new NodeSqliteError(
							"ERR_SQLITE_DESERIALIZE",
							SqlitePrimaryResultCode.SQLITE_ERROR,
							"MessagePack deserialization failed",
							`Failed to deserialize data: ${error instanceof Error ? error.message : String(error)}`,
							error instanceof Error ? error : undefined
						)
					}
				},
			}
		} catch (error) {
			this.#logger?.(
				`Serializer initialization failed: ${error instanceof Error ? error.message : String(error)}`
			)
			if (isNodeSqliteError(error)) {
				throw error
			}
			throw NodeSqliteError.fromNodeSqlite(
				error instanceof Error ? error : new Error(String(error))
			)
		}
	}

	/**
	 * Gets cache statistics if caching is enabled
	 */
	getCacheStats(): CacheStats | undefined {
		this.#logger?.("Retrieving cache statistics")
		return this.#statementCache?.getStats()
	}

	/**
	 * Clears the statement cache if it exists
	 */
	clearStatementCache(): void {
		if (this.#statementCache) {
			this.#logger?.("Clearing statement cache")
			this.#statementCache.clear()
			this.#logger?.("Statement cache cleared")
		}
	}

	close(): void {
		this.#logger?.("Closing database connection")
		if (this.#intervalId) {
			this.#logger?.("Clearing cleanup interval")
			clearInterval(this.#intervalId)
		}
		if (this.#statementCache) {
			this.#logger?.("Clearing statement cache")
			this.#statementCache.clear()
		}
		this.#db.close()
		this.#logger?.("Database connection closed")
	}

	/**
	 * Creates a backup of the database using SQLite's VACUUM INTO.
	 */
	backup(filename: string): void {
		try {
			this.#logger?.(`Starting database backup to ${filename}`)
			this.#db.exec(`VACUUM INTO '${filename}'`)
			this.#logger?.("Database backup completed successfully")
		} catch (error) {
			const sqliteError = NodeSqliteError.fromNodeSqlite(
				error instanceof Error ? error : new Error(String(error))
			)
			if (
				sqliteError.getPrimaryResultCode() ===
				SqlitePrimaryResultCode.SQLITE_CANTOPEN
			) {
				throw new NodeSqliteError(
					"ERR_SQLITE_BACKUP",
					SqlitePrimaryResultCode.SQLITE_CANTOPEN,
					"Cannot create backup file",
					`Failed to create backup at ${filename}. Check permissions and ensure directory exists.`,
					error instanceof Error ? error : undefined
				)
			}
			throw sqliteError
		}
	}

	/**
	 * Restores database from a backup file using SQLite's ATTACH DATABASE.
	 */

	// In database.ts

	/**
	 * Restores database from a backup file
	 */
	restore(filename: string): void {
		try {
			this.#logger?.(`Starting database restore from ${filename}`)

			// Don't try to restore for in-memory databases
			if (this.#location === ":memory:") {
				throw new NodeSqliteError(
					"ERR_SQLITE_RESTORE",
					SqlitePrimaryResultCode.SQLITE_MISUSE,
					"Cannot restore in-memory database",
					"Restore operation is not supported for in-memory databases",
					undefined
				)
			}

			try {
				accessSync(filename)
			} catch (error) {
				throw new NodeSqliteError(
					"ERR_SQLITE_CANTOPEN",
					SqlitePrimaryResultCode.SQLITE_CANTOPEN,
					"Cannot open backup file",
					`Failed to restore from ${filename}. File may not exist or be inaccessible.`,
					error instanceof Error ? error : undefined
				)
			}

			// Close existing database connection first
			this.close()

			// Create a temporary backup of current DB (in case restore fails)
			const tempBackup = join(tmpdir(), `temp-${Date.now()}.db`)

			try {
				// Move current DB to temp backup
				renameSync(this.#location, tempBackup)

				// Move backup into place
				renameSync(filename, this.#location)

				// Open new database
				this.#db = new DatabaseSync(this.#location, { open: true })

				// Verify database integrity
				const integrityCheck = this.#db
					.prepare("PRAGMA integrity_check;")
					.get() as { integrity_check: string }
				if (integrityCheck.integrity_check !== "ok") {
					throw new Error("Database integrity check failed after restore")
				}

				// Verify schema exists
				const schemaCheck = this.#db
					.prepare("SELECT count(*) as count FROM sqlite_master;")
					.get() as { count: number }
				if (schemaCheck.count === 0) {
					throw new Error("Restored database appears to be empty")
				}

				// Remove temp backup on success
				try {
					unlinkSync(tempBackup)
				} catch (error) {
					// Log but don't fail if temp cleanup fails
					this.#logger?.(
						`Warning: Failed to remove temp backup ${tempBackup}: ${error}`
					)
				}
			} catch (error) {
				// Restore failed - put original back
				this.#logger?.("Restore failed, rolling back to original database")
				try {
					if (this.#db) {
						this.#db.close()
					}
					renameSync(tempBackup, this.#location)
					this.#db = new DatabaseSync(this.#location, { open: true })
				} catch (rollbackError) {
					// If rollback fails, we're in real trouble
					throw new NodeSqliteError(
						"ERR_SQLITE_RESTORE",
						SqlitePrimaryResultCode.SQLITE_ERROR,
						"Restore and rollback both failed",
						`Failed to restore database and rollback failed: ${rollbackError}`,
						rollbackError instanceof Error ? rollbackError : undefined
					)
				}
				throw error
			}

			this.#logger?.("Database restore completed successfully")
		} catch (error) {
			if (error instanceof Error && error.message.includes("ENOENT")) {
				throw new NodeSqliteError(
					"ERR_SQLITE_CANTOPEN",
					SqlitePrimaryResultCode.SQLITE_CANTOPEN,
					"Cannot open backup file",
					`Failed to restore from ${filename}. File may not exist or be inaccessible.`,
					error
				)
			}
			throw error instanceof NodeSqliteError
				? error
				: NodeSqliteError.fromNodeSqlite(
						error instanceof Error ? error : new Error(String(error))
					)
		}
	}

	clearExpired(): void {
		const sql = `SELECT name FROM sqlite_master
               WHERE type='table'
               AND name NOT IN ('sqlite_sequence', 'sqlite_stat1')`
		try {
			const stmt = this.prepareStatement(sql)
			const tables = stmt.all() as Array<{ name: string }>
			for (const { name } of tables) {
				// Destructure name from table object
				this.prepareStatement(
					`DELETE FROM ${name}
         WHERE __expires_at IS NOT NULL
         AND __expires_at < ?`
				).run(Date.now())
			}
		} catch (error) {
			if (isNodeSqliteError(error)) {
				throw error
			}
			throw NodeSqliteError.fromNodeSqlite(
				error instanceof Error ? error : new Error(String(error))
			)
		}
	}

	#startCleanupInterval(cleanupInterval?: TimeString): void {
		if (!cleanupInterval) {
			return
		}
		const interval = parseTimeString(cleanupInterval as TimeString)

		this.#intervalId = setInterval(() => {
			try {
				this.clearExpired()
				this.#logger?.("Cleanup of expired data completed")
			} catch (error) {
				this.#logger?.(
					`Error during cleanup: ${error instanceof Error ? error.message : String(error)}`
				)
				if (isNodeSqliteError(error)) {
					throw NodeSqliteError.fromNodeSqlite(error)
				}

				throw new NodeSqliteError(
					"ERR_SQLITE_CLEANUP",
					SqlitePrimaryResultCode.SQLITE_ERROR,
					"Cleanup error",
					`Error during cleanup: ${error instanceof Error ? error.message : String(error)}`,
					error instanceof Error ? error : undefined
				)
			}
		}, interval)
	}
}

export default LazyDb

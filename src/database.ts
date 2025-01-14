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
import { validate, isValidationErrors } from "./utils.js"
import {
	type SerializerOptions,
	type Entity,
	type DatabaseOptions,
	DatabaseOptions as DatabaseOptionsSchema,
	SerializerOptions as SerializerOptionsSchema,
	type RepositoryOptions,
	type QueryKeyDef,
	validateQueryKeys,
} from "./types.js"
import { Repository } from "./repository.new.js"
import {
	createStatementCache,
	type StatementCache,
	type CacheStats,
} from "./cache.js"
import type stringifyLib from "fast-safe-stringify"
const stringify: typeof stringifyLib.default = createRequire(import.meta.url)(
	"fast-safe-stringify"
).default

class LazyDb {
	readonly #db: DatabaseSync
	readonly #logger?: (message: string) => void
	readonly #timestampEnabled: boolean
	readonly #textDecoder = new TextDecoder()
	readonly #serializer: {
		encode: (obj: unknown) => Uint8Array
		decode: (buf: Uint8Array) => unknown
	}
	readonly #statementCache: StatementCache | undefined

	constructor(options: DatabaseOptions = { location: ":memory:" }) {
		try {
			this.#logger = options.logger
			this.#logger?.(`Opening database at ${options.location}`)

			// Validate options
			const validationResult = validate(DatabaseOptionsSchema, options)
			if (isValidationErrors(validationResult)) {
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
			this.#timestampEnabled = options.timestamps ?? false
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

	repository<T extends { [key: string]: unknown }>(
		name: string,
		options?: RepositoryOptions<T>
	): Repository<Entity<T>> {
		this.#logger?.(`Creating repository: ${name}`)

		// Validate repository name
		if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
			throw new NodeSqliteError(
				"ERR_SQLITE_REPOSITORY",
				SqlitePrimaryResultCode.SQLITE_MISUSE,
				"Invalid repository name",
				`Repository name "${name}" must start with a letter or underscore and contain only alphanumeric characters and underscores`,
				undefined
			)
		}

		// Build CREATE TABLE statement
		const createTableSQL = this.#buildCreateTableSQL(
			name,
			options?.queryKeys as Record<string, QueryKeyDef>
		)

		try {
			// Create table if not exists
			this.#db.exec(createTableSQL)

			// Create indexes for queryable columns
			if (options?.queryKeys) {
				this.#createIndexes(
					name,
					options.queryKeys as Record<string, QueryKeyDef>
				)
			}

			// Return new Repository instance
			return new Repository<T>({
				prepareStatement: this.#prepareStatement,
				serializer: this.#serializer,
				timestamps: this.#timestampEnabled,
				queryKeys: options?.queryKeys,
				logger: options?.logger ?? this.#logger,
				name,
				db: this.#db,
			})
		} catch (error) {
			this.#logger?.(
				`Failed to create repository: ${error instanceof Error ? error.message : String(error)}`
			)
			throw error instanceof NodeSqliteError
				? error
				: NodeSqliteError.fromNodeSqlite(
						error instanceof Error ? error : new Error(String(error))
					)
		}
	}

	#buildCreateTableSQL(
		name: string,
		queryKeys?: Record<string, QueryKeyDef>
	): string {
		// biome-ignore lint/style/noUnusedTemplateLiteral: <explanation>
		const columns = [`_id INTEGER PRIMARY KEY AUTOINCREMENT`]

		if (queryKeys) {
			// Validate query keys schema
			const validationResult = validateQueryKeys({ queryKeys })
			if (isValidationErrors(validationResult)) {
				throw new NodeSqliteError(
					"ERR_SQLITE_SCHEMA",
					SqlitePrimaryResultCode.SQLITE_SCHEMA,
					"Invalid query keys schema",
					`Schema validation failed: ${validationResult.map((e) => e.message).join(", ")}`,
					undefined
				)
			}

			// Add columns for queryable fields
			for (const [field, def] of Object.entries(queryKeys)) {
				const constraints: string[] = []

				if (!def.nullable) {
					constraints.push("NOT NULL")
				}
				if (def.default !== undefined) {
					constraints.push(
						`DEFAULT ${
							typeof def.default === "string" ? `'${def.default}'` : def.default
						}`
					)
				}

				// If index.unique is true, add UNIQUE constraint
				if (typeof def.index === "object" && def.index.unique) {
					constraints.push("UNIQUE")
				}

				columns.push(
					`${field} ${def.type}${constraints.length ? ` ${constraints.join(" ")}` : ""}`
				)
			}
		}

		// Add data BLOB column for non-queryable fields
		columns.push("data BLOB")

		return `CREATE TABLE IF NOT EXISTS ${name} (${columns.join(", ")})`
	}

	#createIndexes(name: string, queryKeys: Record<string, QueryKeyDef>): void {
		for (const [field, def] of Object.entries(queryKeys)) {
			if (!def.index) {
				continue
			}

			const indexName = `idx_${name}_${field}`
			const indexType =
				typeof def.index === "object" && def.index.unique ? "UNIQUE" : ""
			const sql = `CREATE ${indexType} INDEX IF NOT EXISTS ${indexName} ON ${name}(${field})`

			try {
				this.#db.exec(sql)
			} catch (error) {
				this.#logger?.(
					`Failed to create index ${indexName}: ${error instanceof Error ? error.message : String(error)}`
				)
				throw error instanceof NodeSqliteError
					? error
					: NodeSqliteError.fromNodeSqlite(
							error instanceof Error ? error : new Error(String(error))
						)
			}
		}
	}

	#prepareStatement(sql: string): StatementSync {
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
			const validationResult = validate(
				SerializerOptionsSchema,
				serializerOption
			)
			if (isValidationErrors(validationResult)) {
				this.#logger?.("Serializer validation failed")
				throw new NodeSqliteError(
					"ERR_SQLITE_CONFIG",
					SqlitePrimaryResultCode.SQLITE_MISUSE,
					"Invalid serializer configuration",
					`Serializer validation failed: ${validationResult.map((e) => e.message).join(", ")}`,
					undefined
				)
			}

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
			this.#logger?.(
				`Backup failed: ${error instanceof Error ? error.message : String(error)}`
			)
			if (isNodeSqliteError(error)) {
				if (
					error.getPrimaryResultCode() ===
					SqlitePrimaryResultCode.SQLITE_CANTOPEN
				) {
					throw new NodeSqliteError(
						"ERR_SQLITE_BACKUP",
						SqlitePrimaryResultCode.SQLITE_CANTOPEN,
						"Cannot create backup file",
						`Failed to create backup at ${filename}. Check permissions and ensure directory exists.`,
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

	/**
	 * Restores database from a backup file using SQLite's ATTACH DATABASE.
	 */
	restore(filename: string): void {
		try {
			this.#logger?.(`Starting database restore from ${filename}`)
			this.#db.exec("BEGIN TRANSACTION")

			try {
				this.#logger?.("Attaching backup database")
				this.#db.exec(`ATTACH DATABASE '${filename}' AS backup`)

				this.#logger?.("Querying tables from backup")
				const tables = this.#db
					.prepare("SELECT name FROM backup.sqlite_master WHERE type='table'")
					.all() as { name: string }[]
				this.#logger?.(`Found ${tables.length} tables to restore`)

				for (const { name } of tables) {
					if (name !== "sqlite_sequence") {
						this.#logger?.(`Restoring table: ${name}`)
						this.#db.exec(`DELETE FROM main.${name}`)
						this.#db.exec(
							`INSERT INTO main.${name} SELECT * FROM backup.${name}`
						)
					}
				}

				this.#logger?.("Detaching backup database")
				this.#db.exec("DETACH DATABASE backup")

				this.#logger?.("Committing transaction")
				this.#db.exec("COMMIT")

				if (this.#statementCache) {
					this.#logger?.("Clearing statement cache")
					this.#statementCache.clear()
				}

				this.#logger?.("Database restore completed successfully")
			} catch (error) {
				this.#logger?.(
					`Restore failed, rolling back: ${error instanceof Error ? error.message : String(error)}`
				)
				this.#db.exec("ROLLBACK")
				throw error
			}
		} catch (error) {
			this.#logger?.(
				`Database restore operation failed: ${error instanceof Error ? error.message : String(error)}`
			)
			if (isNodeSqliteError(error)) {
				if (
					error.getPrimaryResultCode() ===
					SqlitePrimaryResultCode.SQLITE_CANTOPEN
				) {
					throw new NodeSqliteError(
						"ERR_SQLITE_RESTORE",
						SqlitePrimaryResultCode.SQLITE_CANTOPEN,
						"Cannot open backup file",
						`Failed to restore from ${filename}. Ensure file exists and is readable.`,
						error
					)
				}
				if (
					error.getPrimaryResultCode() === SqlitePrimaryResultCode.SQLITE_NOTADB
				) {
					throw new NodeSqliteError(
						"ERR_SQLITE_RESTORE",
						SqlitePrimaryResultCode.SQLITE_NOTADB,
						"Invalid backup file",
						`File ${filename} is not a valid SQLite database.`,
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
}

export { LazyDb }

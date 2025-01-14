import { DatabaseSync, StatementSync } from "node:sqlite"
import msgpackLite from "msgpack-lite"
import type stringifyLib from "fast-safe-stringify"
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
} from "./types.js"
import type { Repository } from "./repository.js"
import {
	StatementCacheOptions,
	createStatementCache,
	type StatementCache,
	type CacheStats,
} from "./cache.js"

const stringify: typeof stringifyLib.default = createRequire(import.meta.url)(
	"fast-safe-stringify"
).default

class LazyDb {
	readonly #db: DatabaseSync
	readonly #repositories: Map<string, Repository<Entity<unknown>>> = new Map()
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
			// Validate options
			const validationResult = validate(DatabaseOptionsSchema, options)
			if (isValidationErrors(validationResult)) {
				throw new NodeSqliteError(
					"ERR_SQLITE_CONFIG",
					SqlitePrimaryResultCode.SQLITE_MISUSE,
					"Invalid database configuration",
					`Configuration validation failed: ${validationResult.map((e) => e.message).join(", ")}`,
					undefined
				)
			}

			this.#logger = options.logger
			// Initialize database with proper error handling
			this.#db = new DatabaseSync(options.location, { open: true })
			this.#timestampEnabled = options.timestamps ?? false

			// Setup serializer
			this.#serializer = this.#initializeSerializer(
				options.serializer ?? "msgpack"
			)
			// Initialize statement cache
			if (options.statementCache === true) {
				// Use default options
				this.#statementCache = createStatementCache({
					maxSize: 1000,
				})
			} else {
				// Use provided options
				this.#statementCache = createStatementCache(options.statementCache)
			}

			// Apply pragmas based on environment and custom settings
			const environment = options.environment || "development"
			const defaultPragmas = PragmaDefaults[environment]
			const customPragmas = options.pragma || {}

			// Merge default and custom pragmas
			const finalPragmas: PragmaConfig = {
				...defaultPragmas,
				...customPragmas,
			}

			// Configure pragmas with error handling
			this.#configurePragmas(finalPragmas)
		} catch (error) {
			if (isNodeSqliteError(error)) {
				throw error
			}
			throw NodeSqliteError.fromNodeSqlite(
				error instanceof Error ? error : new Error(String(error))
			)
		}
	}

	#prepareStatement(sql: string): StatementSync {
		try {
			const cached = this.#statementCache.get(sql)
			if (cached) {
				return cached
			}

			const stmt = this.#db.prepare(sql)
			this.#statementCache.set(sql, stmt)
			return stmt
		} catch (error) {
			if (isNodeSqliteError(error)) {
				if (
					error.getPrimaryResultCode() === SqlitePrimaryResultCode.SQLITE_NOMEM
				) {
					// Clear cache if we're out of memory
					this.#statementCache.clear()
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
			const statements = getPragmaStatements(config)
			for (const stmt of statements) {
				this.#db.exec(stmt)
			}
		} catch (error) {
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
			const validationResult = validate(
				SerializerOptionsSchema,
				serializerOption
			)
			if (isValidationErrors(validationResult)) {
				throw new NodeSqliteError(
					"ERR_SQLITE_CONFIG",
					SqlitePrimaryResultCode.SQLITE_MISUSE,
					"Invalid serializer configuration",
					`Serializer validation failed: ${validationResult.map((e) => e.message).join(", ")}`,
					undefined
				)
			}

			if (typeof serializerOption === "object") {
				return serializerOption
			}

			if (serializerOption === "json") {
				return {
					encode: (obj: unknown) => {
						try {
							return new Uint8Array(Buffer.from(stringify(obj)))
						} catch (error) {
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
			return {
				encode: (obj: unknown) => {
					try {
						return new Uint8Array(msgpackLite.encode(obj))
					} catch (error) {
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
			if (isNodeSqliteError(error)) {
				throw error
			}
			throw NodeSqliteError.fromNodeSqlite(
				error instanceof Error ? error : new Error(String(error))
			)
		}
	}

	/**
	 * Gets cache statistics
	 */
	getCacheStats(): CacheStats {
		return this.#statementCache.getStats()
	}

	/**
	 * Clears the statement cache
	 */
	clearStatementCache(): void {
		this.#statementCache.clear()
	}

	close(): void {
		for (const repo of this.#repositories.values()) {
			repo.close()
		}
		this.#repositories.clear()
		this.#statementCache.clear()
		this.#db.close()
	}
}

export { LazyDb }

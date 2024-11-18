import type {
	DatabaseOptions,
	RangeOptions,
	RootDatabaseOptionsWithPath,
} from "lmdb"

/**
 * Base document type
 */
export type Document<T = unknown> = {
	_id: string
} & T

/**
 * Error types for all possible database operations
 */
export type DatabaseError =
	| { type: "NOT_FOUND"; message: string; key?: string }
	| { type: "VALIDATION"; message: string; field?: string }
	| { type: "TRANSACTION"; message: string; txId?: number }
	| { type: "CONSTRAINT"; message: string; constraint: string }
	| { type: "IO"; message: string; operation: string }
	| { type: "CORRUPTION"; message: string }
	| { type: "UPDATE_FAILED"; message: string } // Added this type
	| { type: "UNKNOWN"; message: string; original?: unknown }
/**
 * Result type for all operations
 */
export type Result<T, E = DatabaseError> = T | { error: E }

/**
 * Query operator types for different field types
 */
export type ComparisonOperator<T> = T extends number | Date
	?
			| { $eq: T }
			| { $ne: T }
			| { $gt: T }
			| { $gte: T }
			| { $lt: T }
			| { $lte: T }
	: T extends Array<infer U>
		? { $eq: T } | { $ne: T } | { $in: U[] } | { $nin: U[] }
		: T extends string
			? { $eq: T } | { $ne: T } | { $regex: RegExp }
			: { $eq: T } | { $ne: T }

/**
 * Type-safe filter for querying documents
 */
export type Filter<T> = {
	[P in keyof T]?: T[P] | ComparisonOperator<T[P]>
}

/** A function used to generate ids for documents */
export type IdGenerator = () => string

/**
 * Safe database options excluding dupSort
 */
export type SafeDatabaseOptions = Omit<DatabaseOptions, "dupSort"> & {
	idGenerator?: IdGenerator
}
export type SafeRootDatabaseOptionsWithPath = Omit<
	RootDatabaseOptionsWithPath,
	"dupSort" | "maxDbs"
> & { maxCollections?: number; idGenerator?: IdGenerator }

/**
 * Find options combining our range needs with LMDB's native options
 *
 */

export type FindOptions<T, R = T> = RangeOptions & {
	map?: (entry: {
		key: string
		value: T
		version?: number
	}) => R
}
/* Operation statistics
 */
export type OperationStats = {
	duration: number
	scanned: number
	matched: number
	modified?: number
	timestamp: number
}

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
 * Operator types for querying
 */
export type ComparisonOperator = "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte"
export type ArrayOperator = "$in" | "$nin"
export type RegexOperator = "$regex"

/**
 * Condition types for different field types
 */
export type Condition<T> =
	| T
	| { [K in ComparisonOperator]?: T }
	| { [K in ArrayOperator]?: T[] }
	| { [K in RegexOperator]?: RegExp }
	| null

/**
 * Type-safe filter for querying documents
 */
export type Filter<T> = {
	[K in keyof T]?: Condition<T[K]>
}

/**
 * Error types for all database operations
 */
export type ErrorType =
	| "UNKNOWN"
	| "VALIDATION"
	| "CONSTRAINT"
	| "TRANSACTION"
	| "NOT_FOUND"
	| "IO"
	| "CORRUPTION"
	| "UPDATE_FAILED"
	| "OPERATION"

export interface DatabaseError {
	type: ErrorType
	message: string
	field?: string
	constraint?: string
	original?: unknown
	operation?: string
	key?: string
	txId?: number
}

/**
 * Result type for all operations
 */
export type Result<T> = T | { error: DatabaseError }

/**
 * Function type for generating document IDs
 */
export type IdGenerator = () => string

/**
 * Options for database operations
 */
export interface FindOptions<T, R = T> extends RangeOptions {
	limit?: number
	offset?: number
	snapshot?: boolean
	map?: (entry: {
		key: string
		value: T
		version?: number
	}) => R
}

/**
 * Safe database options excluding dupSort
 */
export type SafeDatabaseOptions = Omit<DatabaseOptions, "dupSort"> & {
	idGenerator?: IdGenerator
}

export type SafeRootDatabaseOptionsWithPath = Omit<
	RootDatabaseOptionsWithPath,
	"dupSort" | "maxDbs"
> & {
	maxCollections?: number
	idGenerator?: IdGenerator
}

/**
 * Transaction options
 */
export interface TransactionOptions {
	operation: string
	verification?: boolean
}

/**
 * Operation statistics
 */
export interface OperationStats {
	duration: number
	scanned: number
	matched: number
	modified?: number
	timestamp: number
}

/**
 * Collection operations interface
 */
export interface CollectionOperations<T> {
	get(id: string): Result<T | null>
	findOne(filter: Filter<T>): Result<T | null>
	find<R = T>(filter?: Filter<T>, options?: FindOptions<T, R>): Result<R[]>
	insert(doc: Omit<T, "_id">): Promise<Result<T>>
	insertMany(docs: Array<Omit<T, "_id">>): Promise<Result<T[]>>
	updateOne(
		filter: Filter<T>,
		update: Partial<Omit<T, "_id">>
	): Promise<Result<T | null>>
	updateMany(
		filter: Filter<T>,
		update: Partial<Omit<T, "_id">>
	): Promise<Result<number>>
	removeOne(filter: Filter<T>): Promise<Result<boolean>>
	removeMany(filter: Filter<T>): Promise<Result<number>>
}

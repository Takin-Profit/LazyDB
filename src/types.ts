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
 * Function type for generating document IDs
 */
export type IdGenerator = () => string

/**
 * Options for database find operations
 */
export interface FindOptions<T> extends RangeOptions {
	where?: (entry: T) => boolean // Maps to RangeIterable.filter
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

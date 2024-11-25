import type {
	DatabaseOptions,
	RangeOptions,
	RootDatabaseOptionsWithPath,
} from "lmdb"

/**
 * Base entity type
 */
export type Entity<T = unknown> = {
	_id: string
} & T

/**
 * Function type for generating entity IDs
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
	maxRepositories?: number
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

// Event types for database operations
export type DatabaseEvents = {
	[K in
		| "repository.created"
		| "repository.cleared"
		| "repository.dropped"
		| "repository.closed"]: {
		name: string
	}
} & {
	[K in "backup.started" | "backup.completed"]: {
		path: string
		compact: boolean
	}
} & {
	"backup.failed": { path: string; error: Error }
	"database.cleared": null
	"database.closed": null
}

export type RepositoryEvents<T> = {
	[K in
		| "entity.inserted"
		| "entity.updated"
		| "entity.removed"]: K extends "entity.inserted"
		? { entity: T }
		: K extends "entity.updated"
			? { old: T | null; new: T }
			: { entity: T }
} & {
	[K in
		| "entities.inserted"
		| "entities.updated"
		| "entities.removed"]: K extends "entities.inserted"
		? { entities: T[] }
		: K extends "entities.updated"
			? { count: number }
			: { count: number }
} & {
	[K in "entity.upserted"]: { entity: T; wasInsert: boolean }
} & {
	[K in "entities.upserted"]: {
		entities: T[]
		insertCount: number
		updateCount: number
	}
}

/**
 * A generic event emitter class for managing typed events and listeners.
 *
 * This class allows for the registration of event listeners for specific event types, ensuring type safety through generics. It provides methods to add listeners and emit events, invoking all registered listeners for a given event with the appropriate data type.
 *
 * @template Events - A record type defining the event names and their corresponding data types.
 */
export class TypedEventEmitter<Events extends Record<string, unknown>> {
	private readonly listeners: Map<
		keyof Events,
		Array<(data: Events[keyof Events]) => void>
	> = new Map()

	/**
	 * Registers a listener for a specific event type.
	 *
	 * @param {E} event - The name of the event to listen for.
	 * @param {(data: Events[E]) => void} listener - The callback function to invoke when the event is emitted, receiving the event data.
	 */
	on<E extends keyof Events>(
		event: E,
		listener: (data: Events[E]) => void
	): void {
		const handlers = this.listeners.get(event) || []
		handlers.push(listener as (data: Events[keyof Events]) => void)
		this.listeners.set(event, handlers)
	}

	/**
	 * Emits an event, invoking all registered listeners for that event type.
	 *
	 * @param {E} event - The name of the event to emit.
	 * @param {Events[E]} data - The data to pass to the event listeners.
	 */
	protected emit<E extends keyof Events>(event: E, data: Events[E]): void {
		const handlers = this.listeners.get(event)
		if (handlers) {
			for (const handler of handlers) {
				handler(data)
			}
		}
	}
}

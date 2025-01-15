/**
 * Extract all keys from the query keys array.
 */
type ExtractKeys<T extends any[]> = T extends [infer First, ...infer Rest]
	? First extends string
		? First | ExtractKeys<Rest>
		: First extends [infer Key, any]
			? Key extends string
				? Key | ExtractKeys<Rest>
				: ExtractKeys<Rest>
			: ExtractKeys<Rest>
	: never

/**
 * Check if a set of keys is distinct.
 */
type IsDistinct<T extends string> = (
	T extends T
		? (k: T) => void
		: never
) extends (k: infer K) => void
	? [K] extends [T]
		? true
		: false
	: false

/**
 * Enforce distinct keys in the array.
 */
type EnforceDistinct<T extends any[]> = IsDistinct<ExtractKeys<T>> extends true
	? T
	: never

/**
 * Query key definition structure.
 */
type QueryKeyDef<T> = {
	unique?: true
	nullable?: true
	default?: T
}

/**
 * Main QueryKeys type.
 */
type QueryKeys<T> = EnforceDistinct<
	Array<string | [keyof T, QueryKeyDef<T[keyof T]>]>
>

type User = {
	id: number
	email: string
	name: string
	age: number
}

// ✅ Valid QueryKeys
const validQueryKeys: QueryKeys<User> = [
	"id",
	"age",
	["email", { unique: true }],
	["name", { nullable: true }],
]

// ❌ Invalid QueryKeys: Duplicate string keys.
const invalidQueryKeys1: QueryKeys<User> = [
	"id",
	"id", // Error: Duplicate "id"
]

// ❌ Invalid QueryKeys: Duplicate tuple keys.
const invalidQueryKeys2: QueryKeys<User> = [
	["email", { unique: true }],
	["email", { nullable: true }], // Error: Duplicate "email"
]

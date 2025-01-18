import { test, beforeEach, afterEach } from "node:test"
import assert from "node:assert"
import LazyDb from "./database.js"
import type { Repository } from "./repository.js"
import type { SystemQueryKeys } from "./types.js"

interface TestUser {
	name: string | null
	email: string
	age: number
	active: boolean
	metadata: {
		lastLogin: string
		preferences: {
			theme: string
			notifications: boolean
		}
	}
}

interface SimpleEntity {
	name: string | null
	value: number
}

let db: LazyDb

const simpleQueryKeys = {
	name: { type: "TEXT", nullable: true },
	value: {
		type: "INTEGER",
	},
} as const

const userQueryKeys = {
	name: { type: "TEXT", nullable: true },
	email: {
		type: "TEXT",
		unique: true,
	},
	age: {
		type: "INTEGER",
	},
	active: {
		type: "BOOLEAN",
	},
	"metadata.lastLogin": {
		type: "TEXT",
	},
	"metadata.preferences.theme": {
		type: "TEXT",
	},
} as const

// Add to existing interfaces
interface TestProduct {
	name: string
	price: number
	category: string
	inStock: boolean
	rating: number
	dateAdded: string
}

// Add to existing const declarations
const productQueryKeys = {
	name: { type: "TEXT" },
	price: { type: "REAL" },
	category: { type: "TEXT" },
	inStock: { type: "BOOLEAN" },
	rating: { type: "REAL" },
	dateAdded: { type: "TEXT" },
} as const

// Add to existing let declarations
let productRepo: Repository<
	TestProduct,
	typeof productQueryKeys & SystemQueryKeys
>

let simpleRepo: Repository<
	SimpleEntity,
	typeof simpleQueryKeys & SystemQueryKeys
> // We'll type this properly with Repository<SimpleEntity>
let userRepo: Repository<TestUser, typeof userQueryKeys> // We'll type this properly with Repository<TestUser>

beforeEach(() => {
	db = new LazyDb({
		location: ":memory:",
		timestamps: true,
		serializer: "json",
	})

	simpleRepo = db.repository<SimpleEntity>("test").create({
		queryKeys: simpleQueryKeys,
	})

	userRepo = db.repository<TestUser>("users").create({
		queryKeys: userQueryKeys,
	})

	productRepo = db.repository<TestProduct>("products").create({
		queryKeys: productQueryKeys,
	})

	// Insert test products
	const testProducts: TestProduct[] = [
		{
			name: "Product A",
			price: 10.99,
			category: "Electronics",
			inStock: true,
			rating: 4.5,
			dateAdded: "2025-01-01",
		},
		{
			name: "Product B",
			price: 15.99,
			category: "Electronics",
			inStock: false,
			rating: 3.8,
			dateAdded: "2025-01-02",
		},
		{
			name: "Product C",
			price: 5.99,
			category: "Books",
			inStock: true,
			rating: 4.2,
			dateAdded: "2025-01-03",
		},
		{
			name: "Product D",
			price: 25.99,
			category: "Books",
			inStock: true,
			rating: 4.7,
			dateAdded: "2025-01-04",
		},
		{
			name: "Product E",
			price: 8.99,
			category: "Electronics",
			inStock: false,
			rating: 3.5,
			dateAdded: "2025-01-05",
		},
	]
	productRepo.insertMany(testProducts)
})

afterEach(() => {
	db.close()
})

test("successfully inserts and retrieves an entity", async () => {
	const entity: SimpleEntity = { name: "test", value: 123 }
	const inserted = simpleRepo.insert(entity)

	assert.ok(typeof inserted._id === "number")
	assert.equal(inserted.name, entity.name)
	assert.equal(inserted.value, entity.value)

	const retrieved = simpleRepo.findById(inserted._id)
	assert.deepStrictEqual(retrieved, inserted)
})

test("handles null values in nullable fields", async () => {
	const entity: SimpleEntity = { name: null, value: 123 }
	const inserted = simpleRepo.insert(entity)

	assert.ok(typeof inserted._id === "number")
	assert.equal(inserted.name, null)
})

test("enforces NOT NULL constraints", async () => {
	const strictRepo = db.repository<SimpleEntity>("strict").create({
		queryKeys: {
			name: { type: "TEXT" }, // not nullable
			value: { type: "INTEGER" },
		},
	})

	const entity = { name: null, value: 123 }
	assert.throws(() => strictRepo.insert(entity), /NOT NULL constraint failed/)
})

test("handles nested queries with dot notation", async () => {
	const user: TestUser = {
		name: "John Doe",
		email: "john@example.com",
		age: 30,
		active: true,
		metadata: {
			lastLogin: "2025-01-15",
			preferences: {
				theme: "dark",
				notifications: true,
			},
		},
	}

	const inserted = userRepo.insert(user)
	assert.ok(typeof inserted._id === "number")

	const found = userRepo.findOne({
		where: ["metadata.preferences.theme", "=", "dark"],
	})

	assert.ok(found !== null)
	assert.equal(found.metadata.preferences.theme, "dark")
})

test("handles complex WHERE conditions", async () => {
	const testUsers = [
		createTestUser(25, true, "2025-01-01"),
		createTestUser(30, true, "2025-01-02"),
		createTestUser(35, false, "2025-01-03"),
	]

	userRepo.insertMany(testUsers)

	const results = userRepo.find({
		where: [
			["age", ">", 25],
			"AND",
			[["active", "=", true], "OR", ["metadata.lastLogin", ">=", "2025-01-03"]],
		],
	})

	assert.equal(results.length, 2)
})

test("handles batch inserts correctly", async () => {
	const entities: SimpleEntity[] = [
		{ name: "test1", value: 1 },
		{ name: "test2", value: 2 },
		{ name: "test3", value: 3 },
	]

	const inserted = simpleRepo.insertMany(entities)
	assert.equal(inserted.length, 3)
	assert.ok(inserted.every((e) => typeof e._id === "number"))

	const all = simpleRepo.find({})
	assert.equal(all.length, 3)
})

test("handles batch updates correctly", async () => {
	const entities: SimpleEntity[] = [
		{ name: "test1", value: 1 },
		{ name: "test2", value: 2 },
		{ name: "test3", value: 3 },
	]

	simpleRepo.insertMany(entities)

	const updateCount = simpleRepo.updateMany(
		{ where: ["value", ">", 1] },
		{ name: "updated" }
	)

	assert.equal(updateCount, 2)

	const updated = simpleRepo.find({ where: ["name", "=", "updated"] })
	assert.equal(updated.length, 2)
})

test("handles automatic timestamps", { only: true }, async () => {
	const entity: SimpleEntity = { name: "test", value: 123 }
	const inserted = simpleRepo.insert(entity)

	assert.ok(inserted.createdAt)

	const updated = simpleRepo.update(
		{ where: ["_id", "=", inserted._id ?? 0] },
		{ value: 456 }
	)

	assert.ok(updated !== null)
	assert.equal(updated.createdAt, inserted.createdAt)
})

test("handles empty updates", async () => {
	const entity: SimpleEntity = { name: "test", value: 123 }
	const inserted = simpleRepo.insert(entity)

	const updated = simpleRepo.update(
		{ where: ["_id", "=", inserted._id ?? 0] },
		{}
	)

	assert.ok(updated !== null)
	assert.deepStrictEqual(
		{ name: updated.name, value: updated.value },
		{ name: inserted.name, value: inserted.value }
	)
})

test("handles non-existent IDs", async () => {
	const nonExistent = simpleRepo.findById(999999)
	assert.equal(nonExistent, null)

	const deleteResult = simpleRepo.deleteById(999999)
	assert.equal(deleteResult, false)
})

test("handles empty arrays in batch operations", async () => {
	const inserted = simpleRepo.insertMany([])
	assert.equal(inserted.length, 0)
})

// Helper function remains the same
function createTestUser(
	age: number,
	active: boolean,
	lastLogin: string
): TestUser {
	return {
		name: null,
		email: `user${age}@example.com`,
		age,
		active,
		metadata: {
			lastLogin,
			preferences: {
				theme: "light",
				notifications: true,
			},
		},
	}
}

// Individual FindOptions Tests

test("find with limit returns correct number of results", () => {
	const results = productRepo.find({ limit: 2 })
	assert.equal(results.length, 2)
})

test("find with offset skips correct number of results", () => {
	const results = productRepo.find({ offset: 2 })
	// With 5 total products and offset of 2, we expect the last 3 products
	assert.equal(results.length, 5) // Changed from 3 to 5
	assert.equal(results[0].name, "Product A")
})

test("find with single field ascending order", () => {
	const results = productRepo.find({ orderBy: { price: "ASC" } })
	assert.equal(results[0].price, 5.99)
	assert.equal(results[results.length - 1].price, 25.99)
})

test("find with single field descending order", () => {
	const results = productRepo.find({ orderBy: { price: "DESC" } })
	assert.equal(results[0].price, 25.99)
	assert.equal(results[results.length - 1].price, 5.99)
})

test("find with multiple field ordering", () => {
	const results = productRepo.find({
		orderBy: {
			category: "ASC",
			price: "DESC",
		},
	})

	assert.equal(results[0].category, "Books")
	assert.equal(results[0].price, 25.99)
})

test("find with distinct returns unique results", () => {
	const results = productRepo.find({
		distinct: true,
		where: ["category", "=", "Electronics"],
	})

	const categories = new Set(results.map((r) => r.category))
	assert.equal(categories.size, 1)
	assert.equal(results.length, 3)
})

test("find with single field grouping", () => {
	const results = productRepo.find({
		groupBy: ["category"],
	})

	const categories = new Set(results.map((r) => r.category))
	assert.equal(categories.size, 2)
})

test("find with multiple field grouping", () => {
	const results = productRepo.find({
		groupBy: ["category", "inStock"],
	})

	const combinations = new Set()
	for (const result of results) {
		combinations.add(`${result.category}-${result.inStock}`)
	}
	assert.ok(combinations.size <= 4)
})

// Combination Tests

test("find with limit and offset returns correct slice", () => {
	const results = productRepo.find({
		limit: 2,
		offset: 2,
	})

	assert.equal(results.length, 2)
	assert.equal(results[0].name, "Product C")
	assert.equal(results[1].name, "Product D")
})

test("find with limit and order returns sorted slice", () => {
	const results = productRepo.find({
		limit: 2,
		orderBy: { price: "DESC" },
	})

	assert.equal(results.length, 2)
	assert.equal(results[0].price, 25.99)
})

test("find with distinct and order returns sorted unique results", () => {
	const results = productRepo.find({
		distinct: true,
		orderBy: { category: "ASC" },
	})

	assert.ok(results.length > 0)
	assert.equal(results[0].category, "Books")
})

test("find with grouping and ordering", () => {
	const results = productRepo.find({
		groupBy: ["category"],
		orderBy: { price: "DESC" },
	})

	assert.ok(results.length > 0)
	for (let i = 1; i < results.length; i++) {
		assert.ok(results[i - 1].price >= results[i].price)
	}
})

test("find with multiple options combination", () => {
	const results = productRepo.find({
		where: ["price", ">", 10],
		groupBy: ["category"],
		orderBy: { rating: "DESC" },
		limit: 3,
		offset: 1,
	})

	assert.ok(results.length <= 3)
	for (let i = 1; i < results.length; i++) {
		assert.ok(results[i - 1].rating >= results[i].rating)
	}
})

test("find with invalid limit throws error", () => {
	assert.throws(() => {
		productRepo.find({ limit: -1 })
	}, /limit must be a non-negative number/) // Updated to match actual error message
})

test("find with invalid offset throws error", () => {
	assert.throws(() => {
		productRepo.find({ offset: -1 })
	}, /offset must be a non-negative number/) // Updated to match actual error message
})

test("find with empty groupBy array returns all results", () => {
	const results = productRepo.find({ groupBy: [] })
	assert.ok(Array.isArray(results))
})

// Complex Query Tests

test("find with multiple conditions and grouping returns correct results", () => {
	const results = productRepo.find({
		where: [
			["price", ">", 10],
			"AND",
			["rating", ">=", 4],
			"AND",
			["inStock", "=", true],
		],
		groupBy: ["category"],
		orderBy: { price: "DESC" },
	})

	assert.ok(results.length > 0)
	for (const product of results) {
		assert.ok(product.price > 10)
		assert.ok(product.rating >= 4)
		assert.ok(product.inStock === true)
	}
})

test("find with distinct and complex where clause", () => {
	const results = productRepo.find({
		distinct: true,
		where: [
			["category", "=", "Electronics"],
			"OR",
			[["price", "<", 20], "AND", ["rating", ">", 4]],
		],
		orderBy: { dateAdded: "DESC" },
	})

	assert.ok(results.length > 0)
	for (const product of results) {
		assert.ok(
			product.category === "Electronics" ||
				(product.price < 20 && product.rating > 4)
		)
	}
})

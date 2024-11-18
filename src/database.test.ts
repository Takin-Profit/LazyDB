import { strict as assert } from "node:assert"
import { after, before, beforeEach, test } from "node:test"
import { type Collection, isError } from "./collection.js"
import { Database } from "./database.js"
import type { Document } from "./types.js"

const TEST_TIMEOUT = 10000 // 10 seconds timeout for all tests

type User = Document<{
	name: string
	email: string
	age: number
	interests?: string[]
	createdAt?: Date
	lastLoginAt?: Date | null
	status?: "active" | "inactive"
	metadata?: Record<string, unknown>
}>

let db: Database
let users: Collection<User>

const sampleUsers: Array<Omit<User, "_id">> = [
	{
		name: "Alice",
		email: "alice@example.com",
		age: 25,
		interests: ["reading", "traveling"],
		createdAt: new Date("2023-01-01"),
		status: "active",
	},
	{
		name: "Bob",
		email: "bob@example.com",
		age: 30,
		interests: ["gaming", "music"],
		createdAt: new Date("2023-02-15"),
		status: "active",
	},
	{
		name: "Charlie",
		email: "charlie@example.com",
		age: 22,
		interests: ["sports"],
		createdAt: new Date("2023-03-10"),
		status: "inactive",
		lastLoginAt: null,
	},
]

before(async () => {
	db = new Database("test-db", { maxCollections: 5, logger: console.log })
	const collection = db.collection<User>("users")
	if (isError(collection)) {
		throw new Error(`Failed to create collection: ${collection.error.message}`)
	}
	users = collection
})

beforeEach(async () => {
	await db.clearCollection("users")
})

after(async () => {
	try {
		await db.clearCollection("users")
		await users.committed
		await users.flushed
		await db.dropCollection("users")
		await db.close()
	} catch (err) {
		console.error("Failed to clean up database:", err)
		throw err
	}
})

// Basic CRUD Operations
test("Insert operations", { timeout: TEST_TIMEOUT }, async (t) => {
	await t.test("insertMany - multiple users", async () => {
		const result = await users.insertMany(sampleUsers)
		assert(!isError(result))
		assert.equal(result.length, sampleUsers.length)

		for (const user of result) {
			assert(user._id)
		}
	})

	await t.test("insert - single user with all fields", async () => {
		const inserted = await users.insert({
			name: "Frank",
			email: "frank@example.com",
			age: 40,
			interests: ["coding"],
			createdAt: new Date(),
			status: "active",
		})
		assert(!isError(inserted))
		assert(inserted._id)
		assert.equal(inserted.name, "Frank")
	})

	await t.test("insert - minimal fields", async () => {
		const inserted = await users.insert({
			name: "Minimal",
			email: "minimal@example.com",
			age: 20,
		})
		assert(!isError(inserted))
		assert(inserted._id)
	})
})

// Query Operations
test("Find operations", { timeout: TEST_TIMEOUT }, async (t) => {
	await t.test("find - with $eq and $ne", async () => {
		await users.insertMany(sampleUsers)

		const resultsEq = users.find({ age: { $eq: 30 } })
		assert(!isError(resultsEq))
		assert.equal(resultsEq.asArray.length, 1)

		const resultsNe = users.find({ age: { $ne: 30 } })
		assert(!isError(resultsNe))
		assert.equal(resultsNe.asArray.length, 2)
	})

	await t.test("find - with $gt, $gte, $lt, $lte", async () => {
		await users.insertMany(sampleUsers)

		const resultsGt = users.find({ age: { $gt: 25 } })
		assert(!isError(resultsGt))
		assert.equal(resultsGt.asArray.length, 1)

		const resultsGte = users.find({ age: { $gte: 25 } })
		assert(!isError(resultsGte))
		assert.equal(resultsGte.asArray.length, 2)

		const resultsLt = users.find({ age: { $lt: 25 } })
		assert(!isError(resultsLt))
		assert.equal(resultsLt.asArray.length, 1)

		const resultsLte = users.find({ age: { $lte: 25 } })
		assert(!isError(resultsLte))
		assert.equal(resultsLte.asArray.length, 2)
	})
})

// Update Operations
test("Update operations", { timeout: TEST_TIMEOUT }, async (t) => {
	await t.test("updateOne - existing document", async () => {
		await users.insertMany(sampleUsers)

		const beforeUpdate = users.findOne({ name: { $eq: "Alice" } })
		assert(!isError(beforeUpdate))
		assert(beforeUpdate !== null)

		const result = await users.updateOne(
			{ name: { $eq: "Alice" } },
			{ age: 26, status: "inactive" }
		)
		assert(!isError(result))
		assert(result !== null)
		assert.equal(result.age, 26)
		assert.equal(result.status, "inactive")

		const verify = users.findOne({ name: { $eq: "Alice" } })
		assert(!isError(verify))
		assert(verify !== null)
		assert.equal(verify.age, 26)
		assert.equal(verify.status, "inactive")
	})
})

// Delete Operations
test("Remove operations", { timeout: TEST_TIMEOUT }, async (t) => {
	await t.test("remove - single document", async () => {
		await users.insertMany(sampleUsers)

		const removed = await users.remove({ name: { $eq: "Charlie" } })
		assert(!isError(removed))
		assert.equal(removed, true)

		const check = users.findOne({ name: { $eq: "Charlie" } })
		assert(!isError(check))
		assert.equal(check, null)
	})

	await t.test("removeMany - multiple documents", async () => {
		await users.insertMany(sampleUsers)

		const result = await users.removeMany({ status: "active" })
		assert(!isError(result))
		assert.equal(result, 2)

		const remaining = users.find()
		assert(!isError(remaining))
		assert.equal(remaining.asArray.length, 1)
	})
})

// Edge Cases and Error Handling
test("Edge cases", { timeout: TEST_TIMEOUT }, async (t) => {
	await t.test("handle null values", async () => {
		const inserted = await users.insert({
			name: "Null User",
			email: "null@example.com",
			age: 25,
			lastLoginAt: null,
		})
		assert(!isError(inserted))

		const found = users.findOne({ lastLoginAt: null })
		assert(!isError(found))
		assert(found !== null)
	})

	await t.test("handle empty arrays", async () => {
		const inserted = await users.insert({
			name: "Empty Arrays",
			email: "empty@example.com",
			age: 25,
			interests: [],
		})
		assert(!isError(inserted))

		const found = users.find({ interests: { $in: [] } })
		assert(!isError(found))
		assert.equal(found.asArray.length, 0)
	})
})

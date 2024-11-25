import { strict as assert } from "node:assert"
import { after, before, beforeEach, test } from "node:test"
import type { Repository } from "./repository.js"
import { Database } from "./database.js"
import type { Entity } from "./types.js"

const TEST_TIMEOUT = 5000

type User = Entity<{
	name: string
	email: string
	age: number
	interests: string[]
	status: "active" | "inactive"
	loginCount: number
	lastLogin: Date | null
	metadata?: Record<string, unknown>
}>

let db: Database
let users: Repository<User>

function createUser(index: number): Omit<User, "_id"> {
	return {
		name: `User ${index}`,
		email: `user${index}@example.com`,
		age: 20 + (index % 50),
		interests: [`interest${index}`, `interest${index + 1}`],
		status: index % 2 === 0 ? "active" : "inactive",
		loginCount: index % 10,
		lastLogin: index % 3 === 0 ? new Date() : null,
		metadata: {
			createdAt: new Date(),
			source: "test",
		},
	}
}

before(async () => {
	db = new Database("test-db", {
		maxRepositories: 5,
		compression: true,
	})
	users = db.repository<User>("users")
})

beforeEach(async () => {
	await db.clearRepository("users")
})

after(async () => {
	await db.clearRepository("users")
	await db.dropRepository("users")
	await db.close()
})

test("basic operations", { timeout: TEST_TIMEOUT }, async (t) => {
	await t.test("insert single", async () => {
		const user = createUser(1)
		const result = await users.insert(user)
		assert(result._id)
		assert.equal(result.name, user.name)
	})

	await t.test("insert many", async () => {
		const users1 = [createUser(1), createUser(2)]
		const inserted = await users.insertMany(users1)
		assert.equal(inserted.length, 2)
		assert(inserted[0]._id)
		assert(inserted[1]._id)
		assert.equal(inserted[0].name, users1[0].name)
		assert.equal(inserted[1].name, users1[1].name)
	})

	await t.test("find by id", async () => {
		const user = await users.insert(createUser(1))
		const found = users.findOne({
			where: (entity) => entity._id === user._id,
		})
		assert(found)
		assert.equal(found._id, user._id)
		assert.equal(found.name, user.name)
	})

	await t.test("find by status", async () => {
		await users.insertMany([createUser(0), createUser(1), createUser(2)])
		const active = users.find({
			where: (entity) => entity.status === "active",
		})
		assert.equal(active.asArray.length, 2)
		for (const entity of active.asArray) {
			assert.equal(entity.status, "active")
		}
	})

	await t.test("update one", async () => {
		const user = await users.insert(createUser(1))
		const updated = await users.updateOne(
			{ where: (entity) => entity._id === user._id },
			{ age: 99, status: "inactive" }
		)
		assert(updated)
		assert.equal(updated.age, 99)
		assert.equal(updated.status, "inactive")
		assert.equal(updated._id, user._id)

		const found = users.findOne({
			where: (entity) => entity._id === user._id,
		})
		assert(found)
		assert.equal(found.age, 99)
		assert.equal(found.status, "inactive")
		assert.equal(found._id, user._id)
	})

	await t.test("update many", async () => {
		const entities = await users.insertMany([
			createUser(0),
			createUser(1),
			createUser(2),
		])
		assert.equal(entities.length, 3)

		const updated = await users.updateMany(
			{ where: (entity) => entity.status === "active" },
			{ loginCount: 100 }
		)
		assert.equal(updated, 2)

		const found = users.find({
			where: (entity) => entity.status === "active",
		})
		assert.equal(found.asArray.length, 2)
		for (const entity of found.asArray) {
			assert.equal(entity.loginCount, 100)
		}
	})

	await t.test("remove one", async () => {
		const user = await users.insert(createUser(1))
		assert(user._id)

		const beforeRemove = users.findOne({
			where: (entity) => entity._id === user._id,
		})
		assert(beforeRemove)
		assert.equal(beforeRemove._id, user._id)

		const removed = await users.removeOne({
			where: (entity) => entity._id === user._id,
		})
		assert.equal(removed, true)

		const afterRemove = users.findOne({
			where: (entity) => entity._id === user._id,
		})
		assert.equal(afterRemove, null)
	})

	await t.test("remove many", async () => {
		const entities = await users.insertMany([
			createUser(0),
			createUser(1),
			createUser(2),
		])
		assert.equal(entities.length, 3)

		const beforeCount = users.find({
			where: (entity) => entity.status === "active",
		}).asArray.length
		assert.equal(beforeCount, 2)

		const removed = await users.removeMany({
			where: (entity) => entity.status === "active",
		})
		assert.equal(removed, 2)

		const afterCount = users.find({
			where: (entity) => entity.status === "active",
		}).asArray.length
		assert.equal(afterCount, 0)
	})
})

test("complex queries", { timeout: TEST_TIMEOUT }, async () => {
	const entities = await users.insertMany([
		{ ...createUser(0), age: 25, loginCount: 5 },
		{ ...createUser(1), age: 30, loginCount: 10 },
		{ ...createUser(2), age: 35, loginCount: 15 },
	])
	assert.equal(entities.length, 3)

	const found = users.find({
		where: (entity) =>
			entity.status === "active" &&
			entity.age >= 25 &&
			entity.age <= 35 &&
			entity.loginCount >= 10,
	})

	const results = found.asArray
	assert.equal(results.length, 1)
	assert.equal(results[0].age, 35)
	assert.equal(results[0].loginCount, 15)
})

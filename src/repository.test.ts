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
let simpleRepo: Repository<
	SimpleEntity,
	typeof simpleQueryKeys & SystemQueryKeys
> // We'll type this properly with Repository<SimpleEntity>
let userRepo: Repository<TestUser, typeof userQueryKeys & SystemQueryKeys> // We'll type this properly with Repository<TestUser>

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
	assert.ok(inserted.updatedAt)

	const updated = simpleRepo.update(
		{ where: ["_id", "=", inserted._id ?? 0] },
		{ value: 456 }
	)

	assert.ok(updated !== null)
	assert.notEqual(updated.updatedAt, inserted.updatedAt)
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

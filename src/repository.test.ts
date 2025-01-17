// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { test, describe } from "node:test"
import assert from "node:assert"
import LazyDb from "./database.js"

// Test entity types
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

// Helper function to create database instance
function createTestDb() {
	return new LazyDb({
		location: ":memory:",
		timestamps: true,
		serializer: "json",
	})
}

describe("Repository", async () => {
	describe("Basic CRUD Operations", async () => {
		test("should successfully insert and retrieve an entity", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					value: { type: "INTEGER" },
				},
			})

			const entity: SimpleEntity = { name: "test", value: 123 }
			const inserted = repo.insert(entity)

			assert.ok(typeof inserted._id === "number")
			assert.equal(inserted.name, entity.name)
			assert.equal(inserted.value, entity.value)

			if (typeof inserted._id !== "number") {
				throw new Error("Inserted entity missing ID")
			}

			const retrieved = repo.findById(inserted._id)
			assert.deepStrictEqual(retrieved, inserted)

			db.close()
		})

		test("should handle null values in nullable fields", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					value: { type: "INTEGER" },
				},
			})

			const entity: SimpleEntity = { name: null, value: 123 }
			const inserted = repo.insert(entity)

			assert.ok(typeof inserted._id === "number")
			assert.equal(inserted.name, null)

			db.close()
		})

		test("should enforce NOT NULL constraints", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT" }, // not nullable
					value: { type: "INTEGER" },
				},
			})

			const entity = { name: null, value: 123 }
			assert.throws(() => repo.insert(entity), /NOT NULL constraint failed/)

			db.close()
		})
	})

	describe("Complex Queries", async () => {
		test("should handle nested queries with dot notation", async () => {
			const db = createTestDb()
			const repo = db.repository<TestUser>("users").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					email: { type: "TEXT", unique: true },
					age: { type: "INTEGER" },
					active: { type: "BOOLEAN" },
					"metadata.lastLogin": { type: "TEXT" },
					"metadata.preferences.theme": { type: "TEXT" },
				},
			})

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

			const inserted = repo.insert(user)
			assert.ok(typeof inserted._id === "number")

			// Query by nested field
			const found = repo.findOne({
				where: ["metadata.preferences.theme", "=", "dark"],
			})

			assert.ok(found !== null)
			if (found === null) {
				return
			}
			assert.equal(found.metadata.preferences.theme, "dark")

			db.close()
		})

		test("should handle complex WHERE conditions", async () => {
			const db = createTestDb()
			const repo = db.repository<TestUser>("users").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					age: { type: "INTEGER" },
					active: { type: "BOOLEAN" },
					"metadata.lastLogin": { type: "TEXT" },
				},
			})

			// Insert test data
			const users = [
				createTestUser(25, true, "2025-01-01"),
				createTestUser(30, true, "2025-01-02"),
				createTestUser(35, false, "2025-01-03"),
			]

			for (const user of users) {
				repo.insert(user)
			}

			// Complex query with AND/OR conditions
			const results = repo.find({
				where: [
					["age", ">", 25],
					"AND",
					[
						["active", "=", true],
						"OR",
						["metadata.lastLogin", ">=", "2025-01-03"],
					],
				],
			})

			assert.equal(results.length, 2)

			db.close()
		})
	})

	describe("Batch Operations", async () => {
		test("should handle batch inserts correctly", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					value: { type: "INTEGER" },
				},
			})

			const entities: SimpleEntity[] = [
				{ name: "test1", value: 1 },
				{ name: "test2", value: 2 },
				{ name: "test3", value: 3 },
			]

			const inserted = repo.insertMany(entities)
			assert.equal(inserted.length, 3)
			assert.ok(inserted.every((e) => typeof e._id === "number"))

			const all = repo.find({})
			assert.equal(all.length, 3)

			db.close()
		})

		test("should handle batch updates correctly", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					value: { type: "INTEGER" },
				},
			})

			// Insert test data
			const entities: SimpleEntity[] = [
				{ name: "test1", value: 1 },
				{ name: "test2", value: 2 },
				{ name: "test3", value: 3 },
			]
			repo.insertMany(entities)

			// Update all entities where value > 1
			const updateCount = repo.updateMany(
				{ where: ["value", ">", 1] },
				{ name: "updated" }
			)

			assert.equal(updateCount, 2)

			const updated = repo.find({ where: ["name", "=", "updated"] })
			assert.equal(updated.length, 2)

			db.close()
		})
	})

	describe("Error Handling", async () => {
		test("should handle unique constraint violations", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT", unique: true, nullable: true },
					value: { type: "INTEGER" },
				},
			})

			const entity: SimpleEntity = { name: "unique", value: 1 }
			repo.insert(entity)

			assert.throws(
				() => repo.insert({ name: "unique", value: 2 }),
				/UNIQUE constraint failed/
			)

			db.close()
		})

		test("should handle invalid data types", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					value: { type: "INTEGER" },
				},
			})

			// TypeScript should catch this at compile time, but we test runtime behavior
			assert.throws(
				() => repo.insert({ name: "test", value: Number("not a number") }),
				/Invalid value for INTEGER/
			)

			db.close()
		})
	})

	describe("Timestamps", async () => {
		test("should handle automatic timestamps", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				timestamps: true,
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					value: { type: "INTEGER" },
				},
			})

			const entity: SimpleEntity = { name: "test", value: 123 }
			const inserted = repo.insert(entity)

			assert.ok(inserted.createdAt)
			assert.ok(inserted.updatedAt)

			if (typeof inserted._id !== "number") {
				throw new Error("Inserted entity missing ID")
			}

			// Update the entity
			const updated = repo.update(
				{ where: ["_id", "=", inserted._id] },
				{ value: 456 }
			)

			assert.ok(updated !== null)
			if (updated === null) {
				return
			}

			assert.notEqual(updated.updatedAt, inserted.updatedAt)
			assert.equal(updated.createdAt, inserted.createdAt)

			db.close()
		})
	})

	describe("Edge Cases", async () => {
		test("should handle empty updates", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					value: { type: "INTEGER" },
				},
			})

			const entity: SimpleEntity = { name: "test", value: 123 }
			const inserted = repo.insert(entity)

			if (typeof inserted._id !== "number") {
				throw new Error("Inserted entity missing ID")
			}

			const updated = repo.update({ where: ["_id", "=", inserted._id] }, {})

			assert.ok(updated !== null)
			if (updated === null) {
				return
			}

			assert.deepStrictEqual(
				{ name: updated.name, value: updated.value },
				{ name: inserted.name, value: inserted.value }
			)

			db.close()
		})

		test("should handle non-existent IDs", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					value: { type: "INTEGER" },
				},
			})

			const nonExistent = repo.findById(999999)
			assert.equal(nonExistent, null)

			const deleteResult = repo.deleteById(999999)
			assert.equal(deleteResult, false)

			db.close()
		})

		test("should handle empty arrays in batch operations", async () => {
			const db = createTestDb()
			const repo = db.repository<SimpleEntity>("test").create({
				queryKeys: {
					name: { type: "TEXT", nullable: true },
					value: { type: "INTEGER" },
				},
			})

			const inserted = repo.insertMany([])
			assert.equal(inserted.length, 0)

			db.close()
		})
	})
})

// Helper function to create test users
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

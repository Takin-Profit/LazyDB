// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import test, { afterEach, beforeEach } from "node:test"
import assert from "node:assert"
import LazyDb from "./database.js"
import type { Repository } from "./repository.js"
import type { SystemQueryKeys } from "./types.js"

interface TestEntity {
	title: string
	count: number
	isActive: boolean
	tags: string[] // Test serialized data preservation
}

const queryKeys = {
	title: { type: "TEXT" as const },
	count: { type: "INTEGER" as const },
	isActive: { type: "BOOLEAN" as const },
} as const

let db: LazyDb
let repo: Repository<TestEntity, typeof queryKeys & SystemQueryKeys>

test("updateMany functionality", async (t) => {
	// Setup
	beforeEach(() => {
		db = new LazyDb({
			location: ":memory:",
			serializer: "msgpack",
		})

		repo = db.repository<TestEntity>("test").create({
			queryKeys,
		})
	})

	afterEach(() => {
		db.close()
	})

	await t.test("preserves unmodified fields in batch update", () => {
		// Insert test data
		repo.insertMany([
			{ title: "first", count: 1, isActive: true, tags: ["a", "b"] },
			{ title: "second", count: 2, isActive: true, tags: ["c", "d"] },
		])

		// Update only count field
		const updateCount = repo.updateMany(
			{ where: ["isActive", "=", true] },
			{ count: 100 }
		)

		assert.equal(updateCount, 2, "Should update 2 entities")

		// Verify all entities
		const updated = repo.find({ orderBy: { title: "ASC" } })

		// First entity
		assert.equal(updated[0].title, "first", "Title should be preserved")
		assert.equal(updated[0].count, 100, "Count should be updated")
		assert.equal(updated[0].isActive, true, "isActive should be preserved")
		assert.deepEqual(updated[0].tags, ["a", "b"], "Tags should be preserved")

		// Second entity
		assert.equal(updated[1].title, "second", "Title should be preserved")
		assert.equal(updated[1].count, 100, "Count should be updated")
		assert.equal(updated[1].isActive, true, "isActive should be preserved")
		assert.deepEqual(updated[1].tags, ["c", "d"], "Tags should be preserved")
	})

	await t.test("handles null updates correctly", () => {
		// Insert test data with nullable title
		const queryKeysWithNull = {
			...queryKeys,
			title: { ...queryKeys.title, nullable: true },
		} as const

		const nullableRepo = db.repository<TestEntity>("nullable").create({
			queryKeys: queryKeysWithNull,
		})

		nullableRepo.insertMany([
			{ title: "first", count: 1, isActive: true, tags: ["a"] },
			{ title: "second", count: 2, isActive: true, tags: ["b"] },
		])

		// Update title to null and count
		const updateCount = nullableRepo.updateMany(
			{ where: ["isActive", "=", true] },
			{ title: null as unknown as string, count: 200 }
		)

		assert.equal(updateCount, 2, "Should update 2 entities")

		// Verify updates
		const updated = nullableRepo.find({})
		for (const entity of updated) {
			assert.equal(entity.title, null, "Title should be null")
			assert.equal(entity.count, 200, "Count should be updated")
			assert.equal(entity.isActive, true, "isActive should be preserved")
			assert.ok(entity.tags.length === 1, "Tags array should be preserved")
		}
	})

	await t.test("preserves nested data in batch updates", () => {
		interface NestedEntity {
			name: string
			meta: {
				count: number
				flags: {
					active: boolean
					verified: boolean
				}
			}
		}

		const nestedKeys = {
			name: { type: "TEXT" as const },
			"meta.count": { type: "INTEGER" as const },
			"meta.flags.active": { type: "BOOLEAN" as const },
			"meta.flags.verified": { type: "BOOLEAN" as const },
		} as const

		const nestedRepo = db.repository<NestedEntity>("nested").create({
			queryKeys: nestedKeys,
		})

		// Insert test data
		nestedRepo.insertMany([
			{
				name: "first",
				meta: { count: 1, flags: { active: true, verified: true } },
			},
			{
				name: "second",
				meta: { count: 2, flags: { active: true, verified: false } },
			},
		])

		// Update only meta.count
		const updateCount = nestedRepo.updateMany(
			{ where: ["meta.flags.active", "=", true] },
			{ meta: { count: 300 } } // Partial update
		)

		assert.equal(updateCount, 2, "Should update 2 entities")

		// Verify updates
		const updated = nestedRepo.find({ orderBy: { name: "ASC" } })

		// Check first entity
		assert.equal(updated[0].meta.count, 300, "Count should be updated")
		assert.equal(
			updated[0].meta.flags.active,
			true,
			"Nested flags should be preserved"
		)
		assert.equal(
			updated[0].meta.flags.verified,
			true,
			"Nested flags should be preserved"
		)

		// Check second entity
		assert.equal(updated[1].meta.count, 300, "Count should be updated")
		assert.equal(
			updated[1].meta.flags.active,
			true,
			"Nested flags should be preserved"
		)
		assert.equal(
			updated[1].meta.flags.verified,
			false,
			"Nested flags should be preserved"
		)
	})
})

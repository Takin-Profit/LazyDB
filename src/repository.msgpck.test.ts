// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { test, beforeEach, afterEach } from "node:test"
import assert from "node:assert"
import LazyDb from "./database.js"
import type { Repository } from "./repository.js"
import type { SystemQueryKeys } from "./types.js"
import type { DatabaseOptions } from "./types.js"

// 1) Define a minimal test entity schema
interface ExtendedEntity {
	title: string | null
	score: number
	isActive: boolean
	tags?: string[] // Not used in columns, but part of the data
}

// 2) Query keys for ExtendedEntity
const extendedQueryKeys = {
	title: { type: "TEXT", nullable: true, unique: true },
	score: { type: "REAL" },
	isActive: { type: "BOOLEAN" },
} as const

// We’ll create a separate table name:
const TABLE_NAME = "extended_tests"

// Reusable references:
let db: LazyDb
let repo: Repository<ExtendedEntity, typeof extendedQueryKeys & SystemQueryKeys>

/**
 * Prepare a brand-new LazyDb instance with `serializer: "msgpack"`.
 */
beforeEach(() => {
	// Use msgpack for serializer
	const options: DatabaseOptions = {
		location: ":memory:",
		timestamps: true,
		serializer: "msgpack", // Use MessagePack
	}

	db = new LazyDb(options)

	// Create the repository
	repo = db.repository<ExtendedEntity>(TABLE_NAME).create({
		queryKeys: extendedQueryKeys,
	})
})

/**
 * Close db after each test
 */
afterEach(() => {
	db.close()
})

/**
 * -------------- Additional Test Cases Below --------------
 */

/**
 * 1) Insert Null Into Non-Nullable Field
 *    Here we try to insert `score: null` which is not allowed (score is REAL, not nullable).
 *    We expect an error (constraint error).
 */
test("insert fails if non-nullable field is null", () => {
	const invalidEntity = {
		title: "title ok",
		score: null as unknown as number, // Not allowed
		isActive: true,
	}

	assert.throws(() => {
		repo.insert(invalidEntity) // Should fail at constraint
	}, /NOT NULL/)
})

/**
 * 2) Using 'IS' and 'IS NOT' in a WHERE clause
 *    We can do that by enabling a "title" that can be null, and then checking for
 *    `[ "title", "IS", null ]`
 */
test("find with IS / IS NOT operators", () => {
	// Insert two rows: one with `title=null`, another with a real title
	repo.insert({ title: null, score: 10, isActive: true })
	repo.insert({ title: "Not Null", score: 20, isActive: false })

	// 2a) Where title IS NULL
	const nullResults = repo.find({
		where: ["title", "IS", null],
	})
	assert.equal(nullResults.length, 1)
	assert.equal(nullResults[0].title, null)

	// 2b) Where title IS NOT NULL
	const notNullResults = repo.find({
		where: ["title", "IS NOT", null],
	})
	assert.equal(notNullResults.length, 1)
	assert.equal(notNullResults[0].title, "Not Null")
})

/**
 * 3) Invalid Operators in WHERE clause
 *    If your `where` builder disallows unknown operators (like "SUPER-EQ"),
 *    test it throws an error or a NodeSqliteError, etc.
 */
test("find fails with invalid operator", () => {
	assert.throws(() => {
		repo.find({
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
			where: ["score", "SUPER-EQ" as any, 10],
		})
	}, /Invalid operator|ERR_SQLITE_WHERE/)
})

/**
 * 6) Unique Key violation test
 *    Suppose we define "title" as unique in the schema. If not, we can do a new repo with an actual unique column.
 *    We'll do a quick sample if there's a unique key. Right now there's none in extendedQueryKeys,
 *    so you'd need to set `title: { type: "TEXT", unique: true }` or do it on another repo with a unique field.
 */
test("unique key constraint fails on duplicate insert", () => {
	// We'll demonstrate with "title" if we define it as unique for this test:
	// For example, if we had: title: { type: "TEXT", unique: true }
	// Insert first:
	repo.insert({ title: "UniqueTitle", score: 10, isActive: true })

	// Insert second with same title
	assert.throws(() => {
		repo.insert({ title: "UniqueTitle", score: 99, isActive: false })
	}, /UNIQUE constraint failed|SQLITE_CONSTRAINT/)
})

/**
 * 7) updateMany with no matches
 *    If the WHERE clause doesn't match, we expect 0 updates.
 */
test("updateMany with no matches returns 0", () => {
	repo.insert({ title: "ok", score: 10, isActive: true })

	const updatedCount = repo.updateMany(
		{ where: ["score", ">", 500] }, // no match
		{ isActive: false }
	)
	assert.equal(updatedCount, 0)
})

/**
 * 8) deleteMany with complex conditions
 *    Insert multiple rows, then do a multi-clause delete.
 */
test("deleteMany with multi-clause removes correct rows", () => {
	repo.insert({ title: "A", score: 1, isActive: false })
	repo.insert({ title: "B", score: 2, isActive: true })
	repo.insert({ title: "C", score: 3, isActive: false })

	// Delete rows where (score < 3) OR (isActive = false)
	const deletedCount = repo.deleteMany({
		where: [["score", "<", 3], "OR", ["isActive", "=", false]],
	})
	// That should remove (A, B, C) all, except B has score=2 which is <3
	// => Actually, B => score=2 => <3 => also removed
	// So it removes all 3
	assert.equal(deletedCount, 3)

	const remaining = repo.find({})
	assert.equal(remaining.length, 0)
})

/**
 * 9) partial updates on nested data
 *    If we had nested columns, test that updating one nested field doesn't drop the rest.
 *    (We'll skip it if extended entity doesn't have nested columns in extendedQueryKeys.)
 *    But here's a placeholder if you had them:
 */
// test("partial update on nested field doesn't remove other nested data", () => {
//   // ...
// })

/**
 * 10) Double check "update" on row that does exist vs. row that doesn't exist
 */
test("update on nonexistent row returns null", () => {
	const updated = repo.update(
		{ where: ["_id", "=", 999999] },
		{ title: "nope" }
	)
	assert.equal(updated, null)
})

interface NestedEntity {
	name: string
	metadata: {
		value: number
		flag: boolean
	}
	optionalNested?: {
		tag: string
	}
}

const nestedQueryKeys = {
	name: { type: "TEXT" as const },
	"metadata.value": { type: "INTEGER" as const },
	"metadata.flag": { type: "BOOLEAN" as const },
	"optionalNested.tag": { type: "TEXT" as const, nullable: true },
} as const

test("inserts entity with missing optional nested path", () => {
	const nestedRepo = db.repository<NestedEntity>("nested").create({
		queryKeys: nestedQueryKeys,
	})

	const entity = {
		name: "test",
		metadata: { value: 42, flag: true },
		// optionalNested is omitted
	}

	const inserted = nestedRepo.insert(entity)
	assert.equal(inserted.name, "test")
	assert.equal(inserted.metadata.value, 42)
	assert.equal(inserted.metadata.flag, true)
	assert.equal(inserted.optionalNested, undefined)

	// Verify we can retrieve it
	const found = nestedRepo.findById(inserted._id)
	assert.deepEqual(found, inserted)
})

test("inserts and retrieves complex nested structure", () => {
	interface DeepNested {
		top: {
			middle: {
				bottom: {
					value: number
				}
				sibling: boolean
			}
			other: string
		}
	}

	const deepKeys = {
		"top.middle.bottom.value": { type: "INTEGER" as const },
		"top.middle.sibling": { type: "BOOLEAN" as const },
		"top.other": { type: "TEXT" as const },
	}

	const deepRepo = db.repository<DeepNested>("deep").create({
		queryKeys: deepKeys,
	})

	const entity: DeepNested = {
		top: {
			middle: {
				bottom: { value: 42 },
				sibling: true,
			},
			other: "test",
		},
	}

	const inserted = deepRepo.insert(entity)
	assert.equal(inserted.top.middle.bottom.value, 42)
	assert.equal(inserted.top.middle.sibling, true)
	assert.equal(inserted.top.other, "test")

	const found = deepRepo.findById(inserted._id)
	assert.deepEqual(found, inserted)
})

test("handles null values in nested paths", () => {
	interface NullableNested {
		required: {
			value: number
		}
		optional?: {
			value?: number | null
		}
	}

	const nullableKeys = {
		"required.value": { type: "INTEGER" as const },
		"optional.value": { type: "INTEGER" as const, nullable: true },
	}

	const nullableRepo = db.repository<NullableNested>("nullable").create({
		queryKeys: nullableKeys,
	})

	const entity: NullableNested = {
		required: { value: 42 },
		optional: { value: null },
	}

	const inserted = nullableRepo.insert(entity)
	assert.equal(inserted.required.value, 42)
	assert.equal(inserted.optional?.value, null)
})

test("insertMany handles empty array gracefully", () => {
	const result = repo.insertMany([])
	assert.deepEqual(result, [])
})

test("insertMany rollbacks transaction on partial failure", () => {
	const validEntity = { title: "valid", score: 42, isActive: true }
	// biome-ignore lint/suspicious/noExplicitAny: <explanation>
	const invalidEntity = { title: "valid2", score: null as any, isActive: true } // Will fail

	assert.throws(() => {
		repo.insertMany([validEntity, invalidEntity])
	}, /NOT NULL/)

	// Verify nothing was inserted due to rollback
	const results = repo.find({})
	assert.equal(results.length, 0)
})

test("update preserves unmodified fields", () => {
	const entity = repo.insert({
		title: "original",
		score: 42,
		isActive: true,
	})

	const updated = repo.update(
		{ where: ["_id", "=", entity._id] },
		{ score: 100 }
	)

	assert.equal(updated?.title, "original")
	assert.equal(updated?.score, 100)
	assert.equal(updated?.isActive, true)
})

/* test("updateMany returns correct count and updates all matching entities", () => {
	// Insert multiple entities
	repo.insertMany([
		{ title: "test1", score: 42, isActive: true },
		{ title: "test2", score: 42, isActive: true },
		{ title: "test3", score: 100, isActive: true },
	])

	const updateCount = repo.updateMany(
		{ where: ["score", "=", 42] },
		{ isActive: false }
	)

	assert.equal(updateCount, 2)

	const updated = repo.find({ where: ["isActive", "=", false] })
	assert.equal(updated.length, 2)
})
 */
test("find handles complex where conditions with multiple AND/OR", () => {
	repo.insertMany([
		{ title: "A", score: 10, isActive: true },
		{ title: "B", score: 20, isActive: false },
		{ title: "C", score: 30, isActive: true },
		{ title: "D", score: 40, isActive: false },
	])

	const results = repo.find({
		where: [
			["score", ">", 15],
			"AND",
			[["isActive", "=", true], "OR", ["score", ">=", 40]],
		],
	})

	assert.equal(results.length, 2) // Should match C and D
})

test("find with orderBy and complex where conditions", () => {
	repo.insertMany([
		{ title: "A", score: 10, isActive: true },
		{ title: "B", score: 20, isActive: false },
		{ title: "C", score: 30, isActive: true },
	])

	const results = repo.find({
		where: ["isActive", "=", true],
		orderBy: { score: "DESC" },
	})

	assert.equal(results.length, 2)
	assert.equal(results[0].score, 30) // Highest score first
	assert.equal(results[1].score, 10)
})

test("find with pagination", () => {
	repo.insertMany([
		{ title: "A", score: 10, isActive: true },
		{ title: "B", score: 20, isActive: true },
		{ title: "C", score: 30, isActive: true },
		{ title: "D", score: 40, isActive: true },
	])

	const page1 = repo.find({ limit: 2, offset: 0, orderBy: { score: "ASC" } })
	const page2 = repo.find({ limit: 2, offset: 2, orderBy: { score: "ASC" } })

	assert.equal(page1.length, 2)
	assert.equal(page2.length, 2)
	assert.equal(page1[0].score, 10)
	assert.equal(page2[0].score, 30)
})

test("findOne returns first matching result", () => {
	repo.insertMany([
		{ title: "A", score: 10, isActive: true },
		{ title: "B", score: 20, isActive: true },
	])

	const result = repo.findOne({ where: ["score", ">", 15] })
	assert.ok(result)
	assert.equal(result.title, "B")
})

test("deleteById returns false for non-existent ID", () => {
	const result = repo.deleteById(999)
	assert.equal(result, false)
})

test("handles timestamps correctly", () => {
	const entity = repo.insert({ title: "test", score: 42, isActive: true })

	// Verify timestamps exist and are in correct format
	assert.ok(entity.createdAt)
	assert.ok(Date.parse(entity.createdAt))

	// Update should change updatedAt but not createdAt
	const updated = repo.update(
		{ where: ["_id", "=", entity._id] },
		{ score: 100 }
	)

	assert.equal(updated?.createdAt, entity.createdAt)
})

test("handles mixed updates to nullable and non-nullable fields", () => {
	const entity = repo.insert({
		title: "test",
		score: 42,
		isActive: true,
	})

	const updated = repo.update(
		{ where: ["_id", "=", entity._id] },
		{ title: null, score: 100 }
	)

	assert.equal(updated?.title, null)
	assert.equal(updated?.score, 100)
	assert.equal(updated?.isActive, true)
})

test("findOne returns null for no matches", () => {
	const result = repo.findOne({ where: ["_id", "=", 999] })
	assert.equal(result, null)
})

test("insert preserves arrays in serialized data", () => {
	const entity = repo.insert({
		title: "test",
		score: 42,
		isActive: true,
		tags: ["tag1", "tag2"], // This is stored in serialized data
	})

	const retrieved = repo.findById(entity._id)
	assert.deepEqual(retrieved?.tags, ["tag1", "tag2"])
})

test("handles repository with all nullable fields", () => {
	interface AllNullable {
		field1?: string | null
		field2?: number | null
	}

	const nullableKeys = {
		field1: { type: "TEXT" as const, nullable: true },
		field2: { type: "INTEGER" as const, nullable: true },
	} as const

	const nullableRepo = db.repository<AllNullable>("nullable_test").create({
		queryKeys: nullableKeys,
	})

	const entity = nullableRepo.insert({})
	assert.equal(entity.field1, undefined)
	assert.equal(entity.field2, undefined)

	const found = nullableRepo.findById(entity._id)
	assert.deepEqual(found, entity)
})

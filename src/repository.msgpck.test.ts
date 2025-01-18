// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { test, beforeEach, afterEach } from "node:test"
import assert from "node:assert"
import LazyDb from "./database.js"
import type { Repository } from "./repository.js"
import type { SystemQueryKeys } from "./types.js"
import type { DatabaseOptions } from "./types.js"
import { buildInsertQuery } from "./sql.js"

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

test("debug insert with nested paths", () => {
	interface TestNested {
		name: string
		metadata: {
			value: number
			flag: boolean
		}
	}

	const queryKeys = {
		name: { type: "TEXT" as const },
		"metadata.value": { type: "INTEGER" as const },
		"metadata.flag": { type: "BOOLEAN" as const },
	}

	const nestedRepo = db.repository<TestNested>("nested_test").create({
		queryKeys,
	})

	const entity = {
		name: "test",
		metadata: { value: 42, flag: true },
	}

	// Log every step
	const { sql, values } = buildInsertQuery(
		"nested_test",
		entity,
		queryKeys,
		false
	)
	console.log("\nDebug Insert:", {
		sql,
		values,
		placeholdersInSQL: (sql.match(/\?/g) || []).length,
		providedValues: values.length,
		entityJSON: JSON.stringify(entity),
	})

	const inserted = nestedRepo.insert(entity)

	assert.equal(inserted.name, "test")
	assert.equal(inserted.metadata.value, 42)
	assert.equal(inserted.metadata.flag, true)
})

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

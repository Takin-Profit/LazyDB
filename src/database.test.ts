// Copyright 2024 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { strict as assert } from "node:assert"
import { after, beforeEach, describe, test } from "node:test"
import { Database, type Document } from "./index.js"
import { ValidationError, ConstraintError, NotFoundError } from "./errors.js"
import { mkdir, rm } from "node:fs/promises"

interface TestDoc extends Document {
	name: string
	value: number
}

const TEST_DB_PATH = "test-db-main"
const TEST_BACKUP_PATH = "test-db-backup"

describe("Database Class Tests", async () => {
	let db: Database

	beforeEach(async () => {
		try {
			await rm(TEST_DB_PATH, { recursive: true, force: true })
			await rm(TEST_BACKUP_PATH, { recursive: true, force: true })
		} catch {
			// Ignore errors if directories don't exist
		}
		db = new Database(TEST_DB_PATH)
	})

	after(async () => {
		await db?.close()
		await rm(TEST_DB_PATH, { recursive: true, force: true })
		await rm(TEST_BACKUP_PATH, { recursive: true, force: true })
	})

	await test("Database initialization", () => {
		assert(db instanceof Database)
		assert.throws(
			() => new Database(""),
			(err: Error) =>
				err instanceof ValidationError &&
				err.message === "Database path is required"
		)
	})

	await test("Collection creation and limits", async () => {
		const db = new Database(TEST_DB_PATH, { maxCollections: 2 })

		db.collection<TestDoc>("col1")
		db.collection<TestDoc>("col2")

		assert.throws(
			() => db.collection("col3"),
			(err: Error) =>
				err instanceof ConstraintError &&
				err.message === "Maximum number of collections (2) has been reached"
		)

		assert.throws(
			() => db.collection("col1"),
			(err: Error) =>
				err instanceof ConstraintError &&
				err.message === 'Collection "col1" already exists'
		)

		await db.close()
	})

	await test("Collection operations", async () => {
		const col = db.collection<TestDoc>("test")
		await col.insert({ name: "test", value: 1 })

		await db.clearCollection("test")
		const empty = col.find().asArray
		assert.equal(empty.length, 0)

		assert.rejects(
			() => db.clearCollection("nonexistent"),
			(err: Error) =>
				err instanceof NotFoundError &&
				err.message === 'Collection "nonexistent" not found'
		)

		await db.dropCollection("test")
		assert.rejects(
			() => db.clearCollection("test"),
			(err: Error) =>
				err instanceof NotFoundError &&
				err.message === 'Collection "test" not found'
		)
	})

	await test("Multiple collection management", async () => {
		const col1 = db.collection<TestDoc>("col1")
		const col2 = db.collection<TestDoc>("col2")

		await col1.insert({ name: "doc1", value: 1 })
		await col2.insert({ name: "doc2", value: 2 })

		await db.clearAll()

		assert.equal(col1.find().asArray.length, 0)
		assert.equal(col2.find().asArray.length, 0)
	})

	await test("Database backup", async () => {
		const col = db.collection<TestDoc>("test")
		await col.insert({ name: "test", value: 1 })

		// Create backup directory if it doesn't exist
		await mkdir(TEST_BACKUP_PATH, { recursive: true })

		await db.backup(TEST_BACKUP_PATH)

		const backupDb = new Database(TEST_BACKUP_PATH)
		const backupCol = backupDb.collection<TestDoc>("test")
		const docs = backupCol.find().asArray

		assert.equal(docs.length, 1)
		assert.equal(docs[0].name, "test")
		assert.equal(docs[0].value, 1)

		await backupDb.close()

		assert.rejects(
			() => db.backup(""),
			(err: Error) =>
				err instanceof ValidationError &&
				err.message === "Backup path is required"
		)
	})
	await test("Database close and cleanup", async () => {
		const col = db.collection<TestDoc>("test")
		await col.insert({ name: "test", value: 1 })

		await db.close()

		// Verify database is closed by attempting to create a new collection
		assert.throws(() => db.collection("new"), Error)
	})

	await test("Custom ID generator", async () => {
		let counter = 0
		const customDb = new Database(TEST_DB_PATH, {
			idGenerator: () => `custom-${counter++}`,
		})

		const col = customDb.collection<TestDoc>("test")
		const doc = await col.insert({ name: "test", value: 1 })

		assert.equal(doc._id, "custom-0")
		await customDb.close()
	})
})

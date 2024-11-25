// Copyright 2024 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { strict as assert } from "node:assert"
import { after, beforeEach, describe, test } from "node:test"
import { Database, type Entity } from "./index.js"
import { ValidationError, ConstraintError, NotFoundError } from "./errors.js"
import { mkdir, rm } from "node:fs/promises"

interface TestEntity extends Entity {
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

	await test("Repository creation and limits", async () => {
		const db = new Database(TEST_DB_PATH, { maxRepositories: 2 })

		// Create initial repositories
		const repo1 = db.repository<TestEntity>("repo1")
		const _ = db.repository<TestEntity>("repo2")

		// Test max repositories limit
		assert.throws(
			() => db.repository("repo3"),
			(err: Error) =>
				err instanceof ConstraintError &&
				err.message === "Maximum number of repositories (2) has been reached"
		)

		// Test that we get the same instance back instead of throwing
		const repo1Again = db.repository<TestEntity>("repo1")
		assert.strictEqual(
			repo1,
			repo1Again,
			"Should return the same repository instance"
		)

		await db.close()
	})

	await test("Repository operations", async () => {
		const repo = db.repository<TestEntity>("test")
		await repo.insert({ name: "test", value: 1 })

		await db.clearRepository("test")
		const empty = repo.find().asArray
		assert.equal(empty.length, 0)

		assert.rejects(
			() => db.clearRepository("nonexistent"),
			(err: Error) =>
				err instanceof NotFoundError &&
				err.message === 'Repository "nonexistent" not found'
		)

		await db.dropRepository("test")
		assert.rejects(
			() => db.clearRepository("test"),
			(err: Error) =>
				err instanceof NotFoundError &&
				err.message === 'Repository "test" not found'
		)
	})

	await test("Multiple repository management", async () => {
		const repo1 = db.repository<TestEntity>("repo1")
		const repo2 = db.repository<TestEntity>("repo2")

		await repo1.insert({ name: "doc1", value: 1 })
		await repo2.insert({ name: "doc2", value: 2 })

		await db.clearAll()

		assert.equal(repo1.find().asArray.length, 0)
		assert.equal(repo2.find().asArray.length, 0)
	})

	await test("Database backup", async () => {
		const repo = db.repository<TestEntity>("test")
		await repo.insert({ name: "test", value: 1 })

		// Create backup directory if it doesn't exist
		await mkdir(TEST_BACKUP_PATH, { recursive: true })

		await db.backup(TEST_BACKUP_PATH)

		const backupDb = new Database(TEST_BACKUP_PATH)
		const backupRepo = backupDb.repository<TestEntity>("test")
		const entities = backupRepo.find().asArray

		assert.equal(entities.length, 1)
		assert.equal(entities[0].name, "test")
		assert.equal(entities[0].value, 1)

		await backupDb.close()

		assert.rejects(
			() => db.backup(""),
			(err: Error) =>
				err instanceof ValidationError &&
				err.message === "Backup path is required"
		)
	})

	await test("Database close and cleanup", async () => {
		const repo = db.repository<TestEntity>("test")
		await repo.insert({ name: "test", value: 1 })

		await db.close()

		// Verify database is closed by attempting to create a new repository
		assert.throws(() => db.repository("new"), Error)
	})

	await test("Custom ID generator", async () => {
		let counter = 0
		const customDb = new Database(TEST_DB_PATH, {
			idGenerator: () => `custom-${counter++}`,
		})

		const repo = customDb.repository<TestEntity>("test")
		const entity = await repo.insert({ name: "test", value: 1 })

		assert.equal(entity._id, "custom-0")
		await customDb.close()
	})
})

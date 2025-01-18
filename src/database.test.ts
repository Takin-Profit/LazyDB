/* import { test, beforeEach, afterEach } from "node:test"
import assert from "node:assert"
import { rm, accessSync, rmSync } from "node:fs"
import { join } from "node:path"
import { tmpdir } from "node:os"

import LazyDb from "./database.js"
import { NodeSqliteError, SqlitePrimaryResultCode } from "./errors.js"
import type { Repository } from "./repository.js"
import type { SystemQueryKeys } from "./types.js"
import { buildInsertQuery } from "./sql.js"
import { extractQueryableValues } from "./paths.js"

interface TestEntity {
	name: string
	value: number
}

const testEntityKeys = {
	name: { type: "TEXT" as const },
	value: { type: "INTEGER" as const },
} as const

let db: LazyDb
let dbPath: string
let backupPath: string
let testRepo: Repository<TestEntity, typeof testEntityKeys & SystemQueryKeys>

beforeEach(() => {
	dbPath = join(tmpdir(), `test-${Date.now()}.db`)
	backupPath = join(tmpdir(), `backup-${Date.now()}.db`)
	db = new LazyDb({
		location: dbPath,
		logger: () => {}, // Silent logger for tests
	})
	testRepo = db.repository<TestEntity>("test").create({
		queryKeys: testEntityKeys,
	})
})

afterEach(() => {
	db.close()
	try {
		rm(dbPath, console.error)
		rm(backupPath, console.error)
	} catch {
		// Ignore cleanup errors
	}
})

test("clearExpiredData - clears expired data from all tables", () => {
	const expiredEntity = testRepo.insert(
		{ name: "expired", value: 1 },
		{ ttl: "1ms" }
	)
	const validEntity = testRepo.insert({ name: "valid", value: 2 })

	// Force a longer wait to ensure expiration
	const wait = (ms: number) => {
		const start = Date.now()
		while (Date.now() - start < ms) {
			// busy wait
		}
	}
	wait(5) // wait 5ms instead of 2

	db.clearExpired()

	const expired = testRepo.findById(expiredEntity._id)
	assert.strictEqual(expired, null)
	const valid = testRepo.findById(validEntity._id)
	assert.ok(valid)
	assert.equal(valid.name, "valid")
})

test("_clearExpiredData - handles tables with no expired data", () => {
	const entity = testRepo.insert({ name: "test", value: 1 })

	assert.doesNotThrow(() => {
		db.clearExpired()
	})

	const found = testRepo.findById(entity._id)
	assert.ok(found)
	assert.equal(found.name, "test")
})

test("_clearExpiredData - handles empty tables", () => {
	const _ = db.repository<TestEntity>("empty").create({
		queryKeys: testEntityKeys,
	})

	assert.doesNotThrow(() => {
		db.clearExpired()
	})
})

test("backup - creates valid backup file", () => {
	const entity = testRepo.insert({ name: "test", value: 1 })

	db.backup(backupPath)

	// Verify backup file exists
	assert.doesNotThrow(() => accessSync(backupPath))

	// Create new DB from backup
	const backupDb = new LazyDb({ location: backupPath })
	const backupRepo = backupDb.repository<TestEntity>("test").create({
		queryKeys: testEntityKeys,
	})

	const restored = backupRepo.findById(entity._id)
	assert.ok(restored)
	assert.equal(restored.name, "test")
	assert.equal(restored.value, 1)

	backupDb.close()
})

test("backup - throws on invalid path", () => {
	assert.throws(
		() => db.backup("/invalid/path/backup.db"),
		(error: unknown): error is NodeSqliteError =>
			error instanceof NodeSqliteError && error.code === "ERR_SQLITE_BACKUP"
	)
})

test("backup - handles multiple tables and complex data", () => {
	interface ComplexEntity {
		name: string
		metadata: {
			value: number
			flag: boolean
		}
	}

	const complexKeys = {
		name: { type: "TEXT" as const },
		"metadata.value": { type: "INTEGER" as const },
		"metadata.flag": { type: "BOOLEAN" as const },
	} as const

	const complexRepo = db.repository<ComplexEntity>("complex").create({
		queryKeys: complexKeys,
		timestamps: false,
	})

	// Create the entity WITHOUT the dot notation paths in the insert data
	const entity = complexRepo.insert({
		name: "test",
		metadata: { value: 42, flag: true },
	})

	const { sql, values } = buildInsertQuery(
		"complex",
		entity,
		complexKeys,
		false
	)
	console.log({
		sql,
		values,
		placeholderCount: (sql.match(/\?/g) || []).length,
		valueCount: values.length,
	})

	db.backup(backupPath)

	const backupDb = new LazyDb({ location: backupPath })
	const backupRepo = backupDb.repository<ComplexEntity>("complex").create({
		queryKeys: complexKeys,
		timestamps: false,
	})

	const restored = backupRepo.findById(entity._id)
	assert.ok(restored)
	assert.equal(restored.name, "test")
	assert.deepStrictEqual(restored.metadata, { value: 42, flag: true })

	backupDb.close()
})

test("restore - restores database from backup", () => {
	// Skip for in-memory databases
	if (dbPath === ":memory:") {
		return
	}

	const entity = testRepo.insert({ name: "test", value: 1 })

	db.backup(backupPath)

	// Clear original database
	db.close()
	rmSync(dbPath)

	// Create new empty database
	db = new LazyDb({ location: dbPath })
	const newRepo = db.repository<TestEntity>("test").create({
		queryKeys: testEntityKeys,
	})

	db.restore(backupPath)

	const restored = newRepo.findById(entity._id)
	assert.ok(restored)
	assert.equal(restored.name, "test")
	assert.equal(restored.value, 1)
})

test("restore - throws on invalid backup file", () => {
	assert.throws(
		() => db.restore("/nonexistent/backup.db"),
		(error: unknown): error is NodeSqliteError =>
			error instanceof NodeSqliteError &&
			error.getPrimaryResultCode() === SqlitePrimaryResultCode.SQLITE_CANTOPEN
	)
})

test("restore - throws on in-memory database", () => {
	const memoryDb = new LazyDb({ location: ":memory:" })
	assert.throws(
		() => memoryDb.restore("backup.db"),
		(error: unknown): error is NodeSqliteError =>
			error instanceof NodeSqliteError &&
			error.getPrimaryResultCode() === SqlitePrimaryResultCode.SQLITE_MISUSE
	)
	memoryDb.close()
})

test("restore - handles multiple tables and constraints", () => {
	// Skip for in-memory databases
	if (dbPath === ":memory:") return

	const mainRepo = db.repository<TestEntity>("main").create({
		queryKeys: {
			...testEntityKeys,
			name: { ...testEntityKeys.name, unique: true },
		},
	})

	interface SecondEntity {
		name: string
		refId: number
	}
	const secondKeys = {
		name: { type: "TEXT" as const },
		refId: { type: "INTEGER" as const },
	} as const

	const secondRepo = db.repository<SecondEntity>("second").create({
		queryKeys: secondKeys,
	})

	// Insert test data
	const mainEntity = mainRepo.insert({ name: "unique", value: 1 })
	secondRepo.insert({ name: "second", refId: mainEntity._id })

	// Create backup
	db.backup(backupPath)

	// Clear and recreate database
	db.close()
	rmSync(dbPath)
	db = new LazyDb({ location: dbPath })

	// Restore from backup
	db.restore(backupPath)

	// Recreate repositories
	const restoredMain = db.repository<TestEntity>("main").create({
		queryKeys: {
			...testEntityKeys,
			name: { ...testEntityKeys.name, unique: true },
		},
	})

	const restoredSecond = db.repository<SecondEntity>("second").create({
		queryKeys: secondKeys,
	})

	// Test unique constraint
	try {
		restoredMain.insert({ name: "unique", value: 2 })
		assert.fail("Expected insert to fail with unique constraint violation")
	} catch (error) {
		assert.ok(error instanceof NodeSqliteError)
		assert.ok(error.message.includes("UNIQUE constraint"))
	}

	// Test querying
	const secondEntry = restoredSecond.findOne({
		where: ["refId", "=", mainEntity._id],
	})
	assert.ok(secondEntry)
	assert.equal(secondEntry.name, "second")
})
 */

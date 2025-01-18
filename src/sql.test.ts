// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import test from "node:test"
import assert from "node:assert"
import type { LazyDbColumnType, QueryKeysSchema } from "./types.js"
import {
	buildCreateTableSQL,
	buildInsertQuery,
	createIndexes,
	toSqliteValue,
} from "./sql.js"

test("toSqliteValue - TEXT type handling", async (t) => {
	await t.test("accepts valid string values", () => {
		assert.strictEqual(toSqliteValue("hello", "TEXT"), "hello")
		assert.strictEqual(toSqliteValue("", "TEXT"), "")
	})

	await t.test("rejects non-string values", () => {
		assert.throws(() => toSqliteValue(123, "TEXT"), {
			name: "TypeError",
			message: "Invalid value for TEXT: 123",
		})
		assert.throws(() => toSqliteValue(true, "TEXT"), {
			name: "TypeError",
			message: "Invalid value for TEXT: true",
		})
		assert.throws(() => toSqliteValue(123n, "TEXT"), {
			name: "TypeError",
			message: "Invalid value for TEXT: 123",
		})
	})

	await t.test("handles various string values", () => {
		const testStrings = [
			"normal string",
			"",
			" ",
			"!@#$%^&*()",
			"12345",
			"混合文字",
			"\n\t\r",
			"very long string".repeat(100),
		]

		for (const str of testStrings) {
			const result = toSqliteValue(str, "TEXT")
			assert.strictEqual(result, str)
		}
	})
})

test("toSqliteValue - INTEGER type handling", async (t) => {
	await t.test("accepts valid integer values", () => {
		assert.strictEqual(toSqliteValue(42, "INTEGER"), 42)
		assert.strictEqual(toSqliteValue(-42, "INTEGER"), -42)
		assert.strictEqual(toSqliteValue(0, "INTEGER"), 0)
		assert.strictEqual(toSqliteValue(42n, "INTEGER"), 42n)
		assert.strictEqual(toSqliteValue(true, "INTEGER"), 1)
		assert.strictEqual(toSqliteValue(false, "INTEGER"), 0)
	})

	await t.test("rejects non-integer numeric values", () => {
		assert.throws(() => toSqliteValue(42.5, "INTEGER"), {
			name: "TypeError",
			message: "Invalid value for INTEGER: 42.5 (must be an integer)",
		})
	})

	await t.test("rejects invalid types", () => {
		assert.throws(() => toSqliteValue("42", "INTEGER"), {
			name: "TypeError",
			message: "Invalid value for INTEGER: 42",
		})
	})

	await t.test("handles various integer values", () => {
		const testIntegers = [
			0,
			1,
			-1,
			Number.MAX_SAFE_INTEGER,
			Number.MIN_SAFE_INTEGER,
			42n,
			-42n,
			BigInt(Number.MAX_SAFE_INTEGER),
			BigInt(Number.MIN_SAFE_INTEGER),
		]

		for (const num of testIntegers) {
			const result = toSqliteValue(num, "INTEGER")
			if (typeof num === "bigint") {
				assert.strictEqual(result, num)
			} else if (typeof num === "number") {
				assert.strictEqual(result, num)
				assert(Number.isInteger(result))
			}
		}
	})

	await t.test("handles boolean to integer conversion", () => {
		const booleans = [true, false]
		for (const bool of booleans) {
			const result = toSqliteValue(bool, "INTEGER")
			assert.strictEqual(result, bool ? 1 : 0)
		}
	})
})

test("toSqliteValue - REAL type handling", async (t) => {
	await t.test("accepts valid numeric values", () => {
		assert.strictEqual(toSqliteValue(42.5, "REAL"), 42.5)
		assert.strictEqual(toSqliteValue(-42.5, "REAL"), -42.5)
		assert.strictEqual(toSqliteValue(0, "REAL"), 0)
		assert.strictEqual(toSqliteValue(42, "REAL"), 42)
	})

	await t.test("rejects non-numeric values", () => {
		assert.throws(() => toSqliteValue("42.5", "REAL"), {
			name: "TypeError",
			message: "Invalid value for REAL: 42.5",
		})
		assert.throws(() => toSqliteValue(true, "REAL"), {
			name: "TypeError",
			message: "Invalid value for REAL: true",
		})
		assert.throws(() => toSqliteValue(42n, "REAL"), {
			name: "TypeError",
			message: "Invalid value for REAL: 42",
		})
	})

	await t.test("handles various real numbers", () => {
		const testReals = [
			0.0,
			-0.0,
			1.23,
			-1.23,
			Number.MAX_VALUE,
			Number.MIN_VALUE,
			Number.EPSILON,
			Math.PI,
			Math.E,
		]

		for (const num of testReals) {
			if (Number.isFinite(num)) {
				const result = toSqliteValue(num, "REAL")
				assert.strictEqual(result, num)
			}
		}
	})
})

test("toSqliteValue - BOOLEAN type handling", async (t) => {
	await t.test("accepts valid boolean values", () => {
		assert.strictEqual(toSqliteValue(true, "BOOLEAN"), 1)
		assert.strictEqual(toSqliteValue(false, "BOOLEAN"), 0)
	})

	await t.test("rejects non-boolean values", () => {
		assert.throws(() => toSqliteValue(1, "BOOLEAN"), {
			name: "TypeError",
			message: "Invalid value for BOOLEAN: 1",
		})
		assert.throws(() => toSqliteValue("true", "BOOLEAN"), {
			name: "TypeError",
			message: "Invalid value for BOOLEAN: true",
		})
		assert.throws(() => toSqliteValue(0, "BOOLEAN"), {
			name: "TypeError",
			message: "Invalid value for BOOLEAN: 0",
		})
	})
})

test("toSqliteValue - Edge Cases", async (t) => {
	await t.test("handles extreme numeric values", () => {
		assert.strictEqual(
			toSqliteValue(Number.MAX_SAFE_INTEGER, "INTEGER"),
			Number.MAX_SAFE_INTEGER
		)
		assert.strictEqual(
			toSqliteValue(Number.MIN_SAFE_INTEGER, "INTEGER"),
			Number.MIN_SAFE_INTEGER
		)
		assert.strictEqual(
			toSqliteValue(BigInt(Number.MAX_SAFE_INTEGER) + 1n, "INTEGER"),
			BigInt(Number.MAX_SAFE_INTEGER) + 1n
		)
	})

	await t.test("handles special string values", () => {
		const specialStrings = [
			"",
			" ",
			"\0",
			"\u0000",
			"\n",
			"\t",
			"\r",
			"\u200B", // zero-width space
			"\uFEFF", // byte order mark
			"".padStart(1000, "a"), // very long string
		]

		for (const str of specialStrings) {
			assert.strictEqual(toSqliteValue(str, "TEXT"), str)
		}
	})

	await t.test("rejects invalid column types", () => {
		assert.throws(() => toSqliteValue("test", "INVALID" as LazyDbColumnType), {
			name: "TypeError",
			message: "Unsupported SQLite column type: INVALID",
		})
	})
})

test("toSqliteValue - Type Consistency", async (t) => {
	await t.test("type conversion is idempotent", () => {
		// Replace property test with specific test cases
		const testCases = [
			{ value: "test string", type: "TEXT" as const },
			{ value: 42, type: "INTEGER" as const },
			{ value: 3.14, type: "REAL" as const },
			{ value: true, type: "BOOLEAN" as const },
			{ value: 42n, type: "INTEGER" as const },
		]

		for (const { value, type } of testCases) {
			const firstConversion = toSqliteValue(value, type)
			const secondConversion = toSqliteValue(value, type)
			assert.deepStrictEqual(secondConversion, firstConversion)
		}
	})

	await t.test("integer conversions preserve value", () => {
		const integers = [-100, -10, -1, 0, 1, 10, 100, 1000]
		for (const num of integers) {
			const result = toSqliteValue(num, "INTEGER")
			assert.strictEqual(result, num)
			assert(Number.isInteger(result))
		}
	})
})

test("toSqliteValue - Edge Cases", async (t) => {
	await t.test("handles extreme numeric values", () => {
		assert.strictEqual(
			toSqliteValue(Number.MAX_SAFE_INTEGER, "INTEGER"),
			Number.MAX_SAFE_INTEGER
		)
		assert.strictEqual(
			toSqliteValue(Number.MIN_SAFE_INTEGER, "INTEGER"),
			Number.MIN_SAFE_INTEGER
		)
		assert.strictEqual(
			toSqliteValue(BigInt(Number.MAX_SAFE_INTEGER) + 1n, "INTEGER"),
			BigInt(Number.MAX_SAFE_INTEGER) + 1n
		)
	})

	await t.test("handles special string values", () => {
		assert.strictEqual(toSqliteValue("", "TEXT"), "")
		assert.strictEqual(toSqliteValue(" ", "TEXT"), " ")
		assert.strictEqual(toSqliteValue("\0", "TEXT"), "\0")
		assert.strictEqual(toSqliteValue("\u0000", "TEXT"), "\u0000")
	})

	await t.test("rejects invalid column types", () => {
		assert.throws(() => toSqliteValue("test", "INVALID" as LazyDbColumnType), {
			name: "TypeError",
			message: "Unsupported SQLite column type: INVALID",
		})
	})
})

test("toSqliteValue - Comprehensive Type Checks", async (t) => {
	// Create all possible combinations of types and values
	const types: LazyDbColumnType[] = ["TEXT", "INTEGER", "REAL", "BOOLEAN"]
	const values = [
		"string",
		42,
		42.5,
		true,
		false,
		42n,
		0,
		-1,
		Number.MAX_SAFE_INTEGER,
		Number.MIN_SAFE_INTEGER,
	]

	for (const type of types) {
		for (const value of values) {
			await t.test(`handling ${typeof value} value for ${type} type`, () => {
				try {
					const result = toSqliteValue(value, type)

					// Verify the result type based on the column type
					switch (type) {
						case "TEXT":
							assert(typeof result === "string")
							break
						case "INTEGER":
							assert(typeof result === "number" || typeof result === "bigint")
							// Additionally verify if it's a number, it must be an integer
							if (typeof result === "number") {
								assert(Number.isInteger(result))
							}
							break
						case "REAL":
							assert(typeof result === "number")
							break
						case "BOOLEAN":
							assert(result === 0 || result === 1)
							break
					}
				} catch (error) {
					// Some combinations should throw - verify it's the expected error
					assert(error instanceof TypeError)
					assert(error.message.includes(`Invalid value for ${type}`))
				}
			})
		}
	}
})

test("buildCreateTableSQL", async (t) => {
	await t.test("creates basic table without query keys", () => {
		const sql = buildCreateTableSQL("test_table")
		assert.strictEqual(
			sql,
			"CREATE TABLE IF NOT EXISTS test_table (_id INTEGER PRIMARY KEY AUTOINCREMENT, __lazy_data BLOB, __expires_at INTEGER)"
		)
	})

	await t.test("creates table with query keys", () => {
		const queryKeys: QueryKeysSchema<{
			name: string
			age: number
			active: boolean
		}> = {
			name: { type: "TEXT" },
			age: { type: "INTEGER" },
			active: { type: "BOOLEAN" },
		}

		const sql = buildCreateTableSQL("test_table", queryKeys)
		assert.strictEqual(
			sql,
			"CREATE TABLE IF NOT EXISTS test_table (_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, age INTEGER NOT NULL, active BOOLEAN NOT NULL, __lazy_data BLOB, __expires_at INTEGER)"
		)
	})

	await t.test("handles NOT NULL constraint", () => {
		const queryKeys: QueryKeysSchema<{ required: string; optional: string }> = {
			required: { type: "TEXT" },
			optional: { type: "TEXT", nullable: true },
		}

		const sql = buildCreateTableSQL("test_table", queryKeys)
		assert.ok(sql.includes("required TEXT NOT NULL"))
		assert.ok(sql.includes("optional TEXT"))
		assert.ok(!sql.includes("optional TEXT NOT NULL"))
	})

	await t.test("handles DEFAULT values", () => {
		const queryKeys = {
			name: { type: "TEXT", default: "unnamed", nullable: true },
			count: { type: "INTEGER", default: 0, nullable: true },
			active: { type: "BOOLEAN", default: true, nullable: true },
		}

		const sql = buildCreateTableSQL(
			"test_table",
			queryKeys as QueryKeysSchema<{
				name: string
				count: number
				active: boolean
			}>
		)
		assert.ok(sql.includes("name TEXT DEFAULT 'unnamed'"))
		assert.ok(sql.includes("count INTEGER DEFAULT 0"))
		assert.ok(sql.includes("active BOOLEAN DEFAULT true"))
	})

	await t.test("handles UNIQUE constraint", () => {
		const queryKeys = {
			email: { type: "TEXT", unique: true },
			username: { type: "TEXT", unique: true },
		}

		const sql = buildCreateTableSQL(
			"test_table",
			queryKeys as QueryKeysSchema<{ email: string; username: string }>
		)
		assert.ok(sql.includes("email TEXT NOT NULL UNIQUE"))
		assert.ok(sql.includes("username TEXT NOT NULL UNIQUE"))
	})
})

test("createIndexes", async (t) => {
	await t.test("creates basic indexes", () => {
		const queryKeys = {
			name: { type: "TEXT" },
			age: { type: "INTEGER" },
		} as const
		const indexes = createIndexes(
			"test_table",
			queryKeys as QueryKeysSchema<unknown>
		)
		assert.equal(indexes.length, 3) // Updated to include expires_at index
		assert.ok(
			indexes.includes(
				"CREATE INDEX IF NOT EXISTS idx_test_table_name ON test_table(name)"
			)
		)
		assert.ok(
			indexes.includes(
				"CREATE INDEX IF NOT EXISTS idx_test_table_age ON test_table(age)"
			)
		)
		assert.ok(
			indexes.includes(
				"CREATE INDEX IF NOT EXISTS idx_test_table_expires_at ON test_table(__expires_at)"
			)
		)
	})

	await t.test("creates unique indexes", () => {
		const queryKeys: QueryKeysSchema<unknown> = {
			email: { type: "TEXT", unique: true },
			username: { type: "TEXT", unique: true },
			age: { type: "INTEGER" },
		}
		const indexes = createIndexes("test_table", queryKeys)
		assert.equal(indexes.length, 4) // Updated to include expires_at index
		assert.ok(
			indexes.includes(
				"CREATE UNIQUE INDEX IF NOT EXISTS idx_test_table_email ON test_table(email)"
			)
		)
		assert.ok(
			indexes.includes(
				"CREATE UNIQUE INDEX IF NOT EXISTS idx_test_table_username ON test_table(username)"
			)
		)
		assert.ok(
			indexes.includes(
				"CREATE INDEX IF NOT EXISTS idx_test_table_age ON test_table(age)"
			)
		)
		assert.ok(
			indexes.includes(
				"CREATE INDEX IF NOT EXISTS idx_test_table_expires_at ON test_table(__expires_at)"
			)
		)
	})
})

test("buildCreateTableSQL validations", async (t) => {
	await t.test("includes table names in SQL", () => {
		const tableName = "test_table"
		const sql = buildCreateTableSQL(tableName)
		assert.ok(sql.includes(tableName))
		assert.ok(sql.startsWith("CREATE TABLE IF NOT EXISTS"))
	})

	await t.test("includes all query key columns", () => {
		const tableName = "test_table"
		const queryKeys = {
			field1: { type: "TEXT" },
			field2: { type: "INTEGER" },
		}

		const sql = buildCreateTableSQL(
			tableName,
			queryKeys as QueryKeysSchema<{ field1: string; field2: number }>
		)
		assert.ok(sql.includes("field1"))
		assert.ok(sql.includes("field2"))
	})

	await t.test("validates index creation SQL", () => {
		const queryKeys: QueryKeysSchema<unknown> = {
			field1: { type: "TEXT" },
			field2: { type: "INTEGER" },
		}
		const indexes = createIndexes("test_table", queryKeys)
		assert.equal(indexes.length, 3) // Updated to include expires_at index
		for (const sql of indexes) {
			assert.ok(sql.startsWith("CREATE"))
			assert.ok(sql.includes("INDEX"))
			assert.ok(sql.includes("test_table"))
		}
	})

	await t.test(
		"buildCreateTableSQL includes timestamp columns when enabled",
		() => {
			const sql = buildCreateTableSQL("test_table", undefined, true)
			assert(sql.includes("createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"))
			assert(sql.includes("updatedAt TEXT"))
		}
	)

	await t.test(
		"buildCreateTableSQL excludes timestamp columns when disabled",
		() => {
			const sql = buildCreateTableSQL("test_table", undefined, false)
			assert(!sql.includes("createdAt"))
			assert(!sql.includes("updatedAt"))
		}
	)
})

test("buildInsertQuery", async (t) => {
	const tableName = "test_table"

	await t.test("should build basic insert query", () => {
		const entity = { name: "test" }
		const result = buildInsertQuery(tableName, entity)

		assert.equal(
			result.sql,
			"INSERT INTO test_table (__lazy_data) VALUES (?) RETURNING _id, __lazy_data"
		)
		assert.equal(result.values.length, 0)
	})

	await t.test("should include queryable fields when provided", () => {
		interface TestEntity {
			name: string
			age: number
			active: boolean
		}

		const queryKeys: QueryKeysSchema<TestEntity> = {
			name: { type: "TEXT" },
			age: { type: "INTEGER" },
			active: { type: "BOOLEAN" },
		}

		const entity: TestEntity = {
			name: "John",
			age: 30,
			active: true,
		}

		const result = buildInsertQuery(tableName, entity, queryKeys)

		assert.equal(
			result.sql,
			"INSERT INTO test_table (name, age, active, __lazy_data) VALUES (?, ?, ?, ?) RETURNING _id, __lazy_data"
		)
		assert.equal(result.values.length, 3)
		assert.equal(result.values[0], "John")
		assert.equal(result.values[1], 30)
		assert.equal(result.values[2], 1) // boolean true converts to 1
	})

	await t.test("should handle missing optional fields", () => {
		interface TestEntity {
			name?: string
			age?: number
		}

		const queryKeys: QueryKeysSchema<TestEntity> = {
			name: { type: "TEXT" },
			age: { type: "INTEGER" },
		}

		const entity: Partial<TestEntity> = {
			name: "John",
			// age is omitted
		}

		const result = buildInsertQuery(tableName, entity, queryKeys)

		assert.equal(
			result.sql,
			"INSERT INTO test_table (name, __lazy_data) VALUES (?, ?) RETURNING _id, __lazy_data"
		)
		assert.equal(result.values.length, 1)
		assert.equal(result.values[0], "John")
	})

	await t.test("should handle nullable fields", () => {
		interface TestEntity {
			name: string | null
			age: number | null
		}

		const queryKeys: QueryKeysSchema<TestEntity> = {
			name: { type: "TEXT", nullable: true },
			age: { type: "INTEGER", nullable: true },
		}

		const entity: TestEntity = {
			name: null,
			age: null,
		}

		const result = buildInsertQuery(tableName, entity, queryKeys)

		assert.equal(
			result.sql,
			"INSERT INTO test_table (name, age, __lazy_data) VALUES (?, ?, ?) RETURNING _id, __lazy_data"
		)
		assert.equal(result.values.length, 2)
		assert.equal(result.values[0], null)
		assert.equal(result.values[1], null)
	})

	await t.test("should ignore fields not in queryKeys", () => {
		interface TestEntity {
			name: string
			extraField: string // not in queryKeys
		}

		const queryKeys: QueryKeysSchema<TestEntity> = {
			name: { type: "TEXT" },
		}

		const entity: TestEntity = {
			name: "John",
			extraField: "should not be included",
		}

		const result = buildInsertQuery(tableName, entity, queryKeys)

		assert.equal(
			result.sql,
			"INSERT INTO test_table (name, __lazy_data) VALUES (?, ?) RETURNING _id, __lazy_data"
		)
		assert.equal(result.values.length, 1)
		assert.equal(result.values[0], "John")
	})
})

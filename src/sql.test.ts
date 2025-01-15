// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import test, { describe, it } from "node:test"
import assert from "node:assert"
import fc from "fast-check"
import type { LazyDbColumnType, QueryKeys } from "./types.js"
import {
	buildCreateTableSQL,
	buildInsertManyQuery,
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

	// Property test for TEXT
	await t.test("property: all strings are valid TEXT values", () => {
		fc.assert(
			fc.property(fc.string(), (str) => {
				const result = toSqliteValue(str, "TEXT")
				assert.strictEqual(result, str)
			})
		)
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

	// Property tests for INTEGER
	await t.test("property: all integers are valid INTEGER values", () => {
		fc.assert(
			fc.property(fc.integer(), (num) => {
				const result = toSqliteValue(num, "INTEGER")
				assert.strictEqual(result, num)
			})
		)
	})

	await t.test("property: all bigints are valid INTEGER values", () => {
		fc.assert(
			fc.property(fc.bigInt(), (num) => {
				const result = toSqliteValue(num, "INTEGER")
				assert.strictEqual(result, num)
			})
		)
	})

	await t.test("property: booleans convert consistently to INTEGER", () => {
		fc.assert(
			fc.property(fc.boolean(), (bool) => {
				const result = toSqliteValue(bool, "INTEGER")
				assert.strictEqual(result, bool ? 1 : 0)
			})
		)
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

	// Property test for REAL
	await t.test("property: all numbers are valid REAL values", () => {
		fc.assert(
			fc.property(fc.double(), (num) => {
				// Handle special cases that might cause issues
				if (Number.isFinite(num)) {
					const result = toSqliteValue(num, "REAL")
					assert.strictEqual(result, num)
				}
			})
		)
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

	// Property test for BOOLEAN
	await t.test("property: boolean conversion is consistent", () => {
		fc.assert(
			fc.property(fc.boolean(), (bool) => {
				const result = toSqliteValue(bool, "BOOLEAN")
				assert.strictEqual(result, bool ? 1 : 0)
			})
		)
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

test("toSqliteValue - Property Tests", async (t) => {
	await t.test("property: type conversion is idempotent", () => {
		fc.assert(
			fc.property(
				fc.oneof(fc.integer(), fc.string(), fc.boolean(), fc.bigInt()),
				(value) => {
					let columnType: LazyDbColumnType
					if (typeof value === "string") {
						columnType = "TEXT"
					} else if (typeof value === "boolean") {
						columnType = "BOOLEAN"
					} else if (typeof value === "bigint") {
						columnType = "INTEGER"
					} else {
						columnType = Number.isInteger(value) ? "INTEGER" : "REAL"
					}

					const firstConversion = toSqliteValue(value, columnType)
					// Converting the same value twice should yield the same result
					assert.deepStrictEqual(
						toSqliteValue(value, columnType),
						firstConversion
					)
				}
			)
		)
	})

	await t.test("property: boolean conversions are binary", () => {
		fc.assert(
			fc.property(fc.boolean(), (bool) => {
				const result = toSqliteValue(bool, "BOOLEAN")
				assert(result === 0 || result === 1)
			})
		)
	})

	await t.test("property: integer conversions preserve value", () => {
		fc.assert(
			fc.property(fc.integer(), (num) => {
				const result = toSqliteValue(num, "INTEGER")
				assert.strictEqual(result, num)
				assert(Number.isInteger(result))
			})
		)
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
			"CREATE TABLE IF NOT EXISTS test_table (_id INTEGER PRIMARY KEY AUTOINCREMENT, __lazy_data BLOB)"
		)
	})

	await t.test("creates table with query keys", () => {
		const queryKeys: QueryKeys<{ name: string; age: number; active: boolean }> =
			{
				name: { type: "TEXT" },
				age: { type: "INTEGER" },
				active: { type: "BOOLEAN" },
			}

		const sql = buildCreateTableSQL("test_table", queryKeys)
		assert.strictEqual(
			sql,
			"CREATE TABLE IF NOT EXISTS test_table " +
				"(_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
				"name TEXT NOT NULL, " +
				"age INTEGER NOT NULL, " +
				"active BOOLEAN NOT NULL, " +
				"__lazy_data BLOB)"
		)
	})

	await t.test("handles NOT NULL constraint", () => {
		const queryKeys: QueryKeys<{ required: string; optional: string }> = {
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
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
			queryKeys as QueryKeys<any>
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
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
			queryKeys as QueryKeys<any>
		)
		assert.ok(sql.includes("email TEXT NOT NULL UNIQUE"))
		assert.ok(sql.includes("username TEXT NOT NULL UNIQUE"))
	})
})

test("createIndexes", async (t) => {
	await t.test("creates basic indexes", () => {
		const queryKeys = {
			field1: { type: "TEXT" },
			field2: { type: "INTEGER" },
		}

		const statements = createIndexes(
			"test_table",
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
			queryKeys as QueryKeys<any>
		)

		assert.strictEqual(statements.length, 2)
		assert.ok(
			statements[0].includes("CREATE INDEX IF NOT EXISTS idx_test_table_field1")
		)
		assert.ok(
			statements[1].includes("CREATE INDEX IF NOT EXISTS idx_test_table_field2")
		)
	})

	await t.test("creates unique indexes", () => {
		const queryKeys = {
			email: { type: "TEXT", unique: true },
			username: { type: "TEXT", unique: true },
			name: { type: "TEXT" },
		}

		const statements = createIndexes(
			"test_table",
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
			queryKeys as QueryKeys<any>
		)
		assert.strictEqual(statements.length, 3)
		assert.ok(
			statements.some(
				(sql) => sql.includes("CREATE UNIQUE INDEX") && sql.includes("email")
			)
		)
		assert.ok(
			statements.some(
				(sql) => sql.includes("CREATE UNIQUE INDEX") && sql.includes("username")
			)
		)
		assert.ok(
			statements.some(
				(sql) =>
					sql.includes("CREATE INDEX") &&
					!sql.includes("UNIQUE") &&
					sql.includes("name")
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

		// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		const sql = buildCreateTableSQL(tableName, queryKeys as QueryKeys<any>)
		assert.ok(sql.includes("field1"))
		assert.ok(sql.includes("field2"))
	})

	await t.test("validates index creation SQL", () => {
		const tableName = "test_table"
		const queryKeys = {
			field1: { type: "TEXT", unique: true },
			field2: { type: "INTEGER" },
		}

		// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		const statements = createIndexes(tableName, queryKeys as QueryKeys<any>)
		assert.strictEqual(statements.length, 2)
		assert.ok(statements[0].includes("field1"))
		assert.ok(statements[0].includes("UNIQUE"))
		assert.ok(statements[1].includes("field2"))
		assert.ok(!statements[1].includes("UNIQUE"))
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

		const queryKeys: QueryKeys<TestEntity> = {
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

		const queryKeys: QueryKeys<TestEntity> = {
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

		const queryKeys: QueryKeys<TestEntity> = {
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

		const queryKeys: QueryKeys<TestEntity> = {
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

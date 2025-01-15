// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import test from "node:test"
import assert from "node:assert"
import fc from "fast-check"
import { toSqliteValue } from "./utils.js"
import type { LazyDbColumnType } from "./types.js"

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

// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
import test from "node:test"
import assert from "node:assert"
import { buildWhereClause } from "./where.js"
import { NodeSqliteError } from "./errors.js"
import type { QueryKeysSchema } from "./types.js"
import type { Where } from "./where.js"

test("buildWhereClause - Simple Conditions", async (t) => {
	type BasicData = {
		name: string
		age: number
		active: boolean
		deletedAt: string
		email: string
	}

	const queryKeys: QueryKeysSchema<BasicData> = {
		name: { type: "TEXT" },
		age: { type: "INTEGER" },
		active: { type: "BOOLEAN" },
		deletedAt: { type: "TEXT" },
		email: { type: "TEXT" },
	}

	await t.test("basic equals condition", () => {
		const result = buildWhereClause<BasicData>(["name", "=", "John"], queryKeys)
		assert.deepStrictEqual(result, {
			sql: "name = ?",
			params: ["John"],
			fields: ["name"],
		})
	})

	await t.test("numeric comparison", () => {
		const result = buildWhereClause<BasicData>(["age", ">", 18], queryKeys)
		assert.deepStrictEqual(result, {
			sql: "age > ?",
			params: [18],
			fields: ["age"],
		})
	})

	await t.test("boolean value", () => {
		const result = buildWhereClause<BasicData>(["active", "=", true], queryKeys)
		assert.deepStrictEqual(result, {
			sql: "active = ?",
			params: [1],
			fields: ["active"],
		})
	})

	await t.test("null comparison with IS", () => {
		const result = buildWhereClause<BasicData>(
			["deletedAt", "IS", null] as unknown as Where<BasicData>,
			queryKeys
		)
		assert.deepStrictEqual(result, {
			sql: "deletedAt IS NULL",
			params: [],
			fields: ["deletedAt"],
		})
	})

	await t.test("LIKE operator", () => {
		const result = buildWhereClause<BasicData>(
			["email", "LIKE", "%@example.com"],
			queryKeys
		)
		assert.deepStrictEqual(result, {
			sql: "email LIKE ?",
			params: ["%@example.com"],
			fields: ["email"],
		})
	})
})

test("buildWhereClause - IN Conditions", async (t) => {
	type StatusData = {
		status: string
		id: number
	}

	const queryKeys: QueryKeysSchema<StatusData> = {
		status: { type: "TEXT" },
		id: { type: "INTEGER" },
	}

	await t.test("IN with array of strings", () => {
		const result = buildWhereClause<StatusData, typeof queryKeys>(
			["status", "IN", ["active", "pending"]],
			queryKeys
		)
		assert.deepStrictEqual(result, {
			sql: "status IN (?, ?)",
			params: ["active", "pending"],
			fields: ["status"],
		})
	})

	await t.test("NOT IN with array of numbers", () => {
		const result = buildWhereClause<StatusData, typeof queryKeys>(
			["id", "NOT IN", [1, 2, 3]],
			queryKeys
		)
		assert.deepStrictEqual(result, {
			sql: "id NOT IN (?, ?, ?)",
			params: [1, 2, 3],
			fields: ["id"],
		})
	})

	await t.test("IN with empty array should throw", () => {
		assert.throws(
			() =>
				buildWhereClause<StatusData, typeof queryKeys>(
					["status", "IN", []],
					queryKeys
				),
			NodeSqliteError
		)
	})
})

test("buildWhereClause - Complex Conditions", async (t) => {
	type ComplexData = {
		age: number
		active: boolean
		status: string
		role: string
	}

	const queryKeys: QueryKeysSchema<ComplexData> = {
		age: { type: "INTEGER" },
		active: { type: "BOOLEAN" },
		status: { type: "TEXT" },
		role: { type: "TEXT" },
	}

	await t.test("simple AND condition", () => {
		const result = buildWhereClause<ComplexData, typeof queryKeys>(
			[["age", ">=", 18], "AND", ["active", "=", true]],
			queryKeys
		)
		assert.deepStrictEqual(result, {
			sql: "(age >= ? AND active = ?)",
			params: [18, 1],
			fields: ["age", "active"],
		})
	})

	await t.test("OR condition", () => {
		const result = buildWhereClause<ComplexData>(
			[["status", "=", "pending"], "OR", ["status", "=", "active"]],
			queryKeys
		)
		assert.deepStrictEqual(result, {
			sql: "(status = ? OR status = ?)",
			params: ["pending", "active"],
			fields: ["status", "status"],
		})
	})

	await t.test("complex AND/OR combination", () => {
		const result = buildWhereClause<ComplexData>(
			[
				["age", ">=", 18],
				"AND",
				["status", "=", "active"],
				"OR",
				["role", "IN", ["admin", "moderator"]],
			],
			queryKeys
		)
		assert.deepStrictEqual(result, {
			sql: "(age >= ? AND status = ? OR role IN (?, ?))",
			params: [18, "active", "admin", "moderator"],
			fields: ["age", "status", "role"],
		})
	})
})

test("buildWhereClause - No QueryKeys", async (t) => {
	await t.test("returns empty result with no queryKeys", () => {
		const result = buildWhereClause<{ field: string }>(["field", "=", "value"])
		assert.deepStrictEqual(result, {
			sql: "",
			params: [],
			fields: [],
		})
	})

	await t.test(
		"returns empty result with complex condition and no queryKeys",
		() => {
			const result = buildWhereClause<{ field1: string; field2: string }>([
				["field1", "=", "value1"],
				"AND",
				["field2", "=", "value2"],
			])
			assert.deepStrictEqual(result, {
				sql: "",
				params: [],
				fields: [],
			})
		}
	)
})

// Keep Type Handling tests as they are - they're working correctly

test("buildWhereClause - Error Cases", async (t) => {
	type ErrorData = {
		age: number
		status: string
		score: number
	}

	const queryKeys: QueryKeysSchema<ErrorData> = {
		age: { type: "INTEGER" },
		status: { type: "TEXT" },
		score: { type: "REAL" },
	}

	await t.test("invalid value type for INTEGER field", () => {
		assert.throws(
			() =>
				buildWhereClause<ErrorData>(
					["age", "=", "not-a-number"] as unknown as Where<ErrorData>,
					queryKeys
				),
			TypeError
		)
	})

	await t.test("invalid value type for REAL field", () => {
		assert.throws(
			() =>
				buildWhereClause<ErrorData>(
					["score", "=", "not-a-number" as unknown as number],
					queryKeys
				),
			TypeError
		)
	})

	await t.test("IS operator with non-null value", () => {
		assert.throws(
			() =>
				buildWhereClause<{ status: string }>(
					["status", "IS", "active"] as unknown as Where<{ status: string }>,
					queryKeys
				),
			NodeSqliteError
		)
	})

	await t.test("IN operator with non-array value", () => {
		assert.throws(
			() =>
				buildWhereClause<{ status: string }>(
					["status", "IN", "active" as unknown as []],
					queryKeys
				),
			NodeSqliteError
		)
	})
})

test("buildWhereClause - Edge Cases", async (t) => {
	type EdgeData = {
		id: number
		field: string
		a: number
		b: number
		c: number
		d: number
		e: number
		name: string
	}

	const queryKeys: QueryKeysSchema<EdgeData> = {
		id: { type: "INTEGER" },
		field: { type: "TEXT" },
		a: { type: "INTEGER" },
		b: { type: "INTEGER" },
		c: { type: "INTEGER" },
		d: { type: "INTEGER" },
		e: { type: "INTEGER" },
		name: { type: "TEXT" },
	}

	await t.test("very long IN clause", () => {
		const values = Array.from({ length: 1000 }, (_, i) => i)
		const result = buildWhereClause<EdgeData>(["id", "IN", values], queryKeys)
		assert.strictEqual(result.params.length, 1000)
		assert.strictEqual(result.sql.split("?").length - 1, 1000)
	})

	await t.test("deeply nested conditions", () => {
		const result = buildWhereClause<EdgeData>(
			[
				["a", "=", 1],
				"AND",
				["b", "=", 2],
				"AND",
				["c", "=", 3],
				"AND",
				["d", "=", 4],
				"AND",
				["e", "=", 5],
			],
			queryKeys
		)
		assert.strictEqual(result.params.length, 5)
		assert.strictEqual(result.fields.length, 5)
	})

	await t.test("special characters in field names", () => {
		const specialQueryKeys: QueryKeysSchema<{ name: string }> = {
			name: { type: "TEXT" },
		}
		const result = buildWhereClause<EdgeData>(
			["name", "=", "test"],
			specialQueryKeys
		)
		assert.strictEqual(result.sql, "name = ?")
	})
})

// Keep Type Enforcement tests as they are - they're working correctly

test("buildWhereClause - Type Enforcement", async (t) => {
	type Data = {
		intField: number
		boolField: boolean
		realField: number
		textField: string
	}
	const queryKeys: QueryKeysSchema<Data> = {
		intField: { type: "INTEGER" },
		boolField: { type: "BOOLEAN" },
		realField: { type: "REAL" },
		textField: { type: "TEXT" },
	}

	await t.test("rejects string for INTEGER field", () => {
		assert.throws(
			() =>
				buildWhereClause<Data>(
					["intField", "=", "123"] as unknown as Where<Data>,
					queryKeys
				),
			{
				message: "Invalid value for INTEGER: 123",
			}
		)
	})

	await t.test("rejects number for TEXT field", () => {
		assert.throws(
			() =>
				buildWhereClause<Data>(
					["textField", "=", 123] as unknown as Where<Data>,
					queryKeys
				),
			{
				message: "Invalid value for TEXT: 123",
			}
		)
	})

	await t.test("rejects string for REAL field", () => {
		assert.throws(
			() =>
				buildWhereClause<Data>(
					["realField", "=", "123.45"] as unknown as Where<Data>,
					queryKeys
				),
			{ message: "Invalid value for REAL: 123.45" }
		)
	})

	await t.test("rejects string for BOOLEAN field", () => {
		assert.throws(
			() =>
				buildWhereClause<Data>(
					["boolField", "=", "true"] as unknown as Where<Data>,
					queryKeys
				),
			{ message: "Invalid value for BOOLEAN: true" }
		)
	})

	await t.test("accepts correct types", () => {
		assert.doesNotThrow(() =>
			buildWhereClause<Data>(["intField", "=", 123], queryKeys)
		)
		assert.doesNotThrow(() =>
			buildWhereClause<Data>(["textField", "=", "hello"], queryKeys)
		)
		assert.doesNotThrow(() =>
			buildWhereClause<Data>(["realField", "=", 123.45], queryKeys)
		)
		assert.doesNotThrow(() =>
			buildWhereClause<Data>(["boolField", "=", true], queryKeys)
		)
	})
})

test("buildWhereClause - Advanced Cases", async (t) => {
	await t.test("logical operator symmetry for OR", () => {
		const queryKeys: QueryKeysSchema<{ a: number; b: number }> = {
			a: { type: "INTEGER" },
			b: { type: "INTEGER" },
		}

		// Test cases with different combinations
		const testCases = [
			{ a: 1, b: 2 },
			{ a: -10, b: 10 },
			{ a: 0, b: 0 },
			{ a: 999, b: -999 },
		]

		for (const { a, b } of testCases) {
			const result1 = buildWhereClause<{ a: number; b: number }>(
				[["a", "=", a], "OR", ["b", "=", b]],
				queryKeys
			)

			const result2 = buildWhereClause<{ a: number; b: number }>(
				[["b", "=", b], "OR", ["a", "=", a]],
				queryKeys
			)

			// For OR operations, params should contain same values regardless of order
			assert.deepStrictEqual(new Set(result1.params), new Set(result2.params))
		}
	})

	await t.test("range query mutual exclusivity", () => {
		const queryKeys: QueryKeysSchema<{ value: number }> = {
			value: { type: "INTEGER" },
		}

		// Test various boundary cases and regular numbers
		const testValues = [
			0,
			1,
			-1,
			Number.MAX_SAFE_INTEGER,
			Number.MIN_SAFE_INTEGER,
			42,
			-42,
		]

		for (const value of testValues) {
			const greaterThan = buildWhereClause<{ value: number }>(
				["value", ">", value],
				queryKeys
			)
			const lessThanOrEqual = buildWhereClause<{ value: number }>(
				["value", "<=", value],
				queryKeys
			)

			// These conditions should be mutually exclusive
			assert.notDeepStrictEqual(greaterThan, lessThanOrEqual)
			// Verify SQL structure
			assert.ok(greaterThan.sql.includes(">"))
			assert.ok(lessThanOrEqual.sql.includes("<="))
		}
	})

	await t.test("null handling consistency", () => {
		const queryKeys: QueryKeysSchema<{ nullable: string | null }> = {
			nullable: { type: "TEXT" },
		}

		// Test all null-related operators
		const isNull = buildWhereClause<{ nullable: string | null }>(
			["nullable", "IS", null],
			queryKeys
		)
		const isNotNull = buildWhereClause<{ nullable: string | null }>(
			["nullable", "IS NOT", null],
			queryKeys
		)

		// Verify basic expectations
		assert.notEqual(isNull.sql, isNotNull.sql)
		assert.strictEqual(isNull.params.length, 0)
		assert.strictEqual(isNotNull.params.length, 0)
		assert.ok(isNull.sql.includes("IS NULL"))
		assert.ok(isNotNull.sql.includes("IS NOT NULL"))
	})

	await t.test("IN clause with various array sizes", () => {
		const queryKeys: QueryKeysSchema<{ ids: number }> = {
			ids: { type: "INTEGER" },
		}

		// Test different array sizes
		const testArrays = [
			[1],
			[1, 2],
			[1, 2, 3, 4, 5],
			Array.from({ length: 10 }, (_, i) => i),
			Array.from({ length: 100 }, (_, i) => i),
		]

		for (const numbers of testArrays) {
			const result = buildWhereClause<{ ids: number }>(
				["ids", "IN", numbers],
				queryKeys
			)

			assert.strictEqual(result.params.length, numbers.length)
			assert.strictEqual(result.sql.split("?").length - 1, numbers.length)
			assert.ok(result.sql.startsWith("ids IN ("))
			assert.ok(result.sql.endsWith(")"))
		}
	})

	await t.test("complex nested conditions", () => {
		const queryKeys: QueryKeysSchema<{ field: number }> = {
			field: { type: "INTEGER" },
		}

		// Test cases with different levels of nesting
		const testCases = [
			{
				numbers: [1, 2],
				expectedParams: 2,
				operator: "AND",
			},
			{
				numbers: [1, 2, 3],
				expectedParams: 3,
				operator: "AND",
			},
			{
				numbers: [1, 2, 3, 4],
				expectedParams: 4,
				operator: "OR",
			},
			{
				numbers: [1, 2, 3, 4, 5],
				expectedParams: 5,
				operator: "AND",
			},
		]

		for (const { numbers, expectedParams, operator } of testCases) {
			const conditions = numbers.map((n) => ["field", "=", n])
			const operators = Array(numbers.length - 1).fill(operator)
			const where = conditions.reduce<unknown[]>((acc, condition, i) => {
				if (i === conditions.length - 1) {
					acc.push(condition)
					return acc
				}
				acc.push(condition, operators[i])
				return acc
			}, [])

			const result = buildWhereClause<{ field: number }>(
				where as Where<{ field: number }>,
				queryKeys
			)

			// Verify structure and content
			assert.strictEqual(result.params.length, expectedParams)
			assert.strictEqual(
				(result.sql.match(/\(/g) || []).length,
				(result.sql.match(/\)/g) || []).length
			)
			for (const n of numbers) {
				assert(result.params.includes(n))
			}
		}
	})

	await t.test("mixed operator chains", () => {
		const queryKeys: QueryKeysSchema<{ a: number; b: number; c: number }> = {
			a: { type: "INTEGER" },
			b: { type: "INTEGER" },
			c: { type: "INTEGER" },
		}

		const result = buildWhereClause<{ a: number; b: number; c: number }>(
			[["a", "=", 1], "AND", ["b", "=", 2], "OR", ["c", "=", 3]],
			queryKeys
		)

		assert.strictEqual(result.params.length, 3)
		assert.ok(result.sql.includes("AND"))
		assert.ok(result.sql.includes("OR"))
		assert.deepStrictEqual(result.params, [1, 2, 3])
	})
})

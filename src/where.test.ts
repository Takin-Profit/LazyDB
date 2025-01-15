// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import test from "node:test"
import assert from "node:assert"
import { buildWhereClause } from "./where.js"
import { NodeSqliteError } from "./errors.js"
import type { QueryKeyDef } from "./types.js"
import type { WhereCondition } from "./where.js"

test("buildWhereClause - Simple Conditions", async (t) => {
	await t.test("basic equals condition", () => {
		const result = buildWhereClause(["name", "=", "John"])
		assert.deepStrictEqual(result, {
			sql: "name = ?",
			params: ["John"],
			fields: ["name"],
		})
	})

	await t.test("numeric comparison", () => {
		const result = buildWhereClause(["age", ">", 18])
		assert.deepStrictEqual(result, {
			sql: "age > ?",
			params: [18],
			fields: ["age"],
		})
	})

	await t.test("boolean value", () => {
		const result = buildWhereClause(["active", "=", true])
		assert.deepStrictEqual(result, {
			sql: "active = ?",
			params: [1],
			fields: ["active"],
		})
	})

	await t.test("null comparison with IS", () => {
		const result = buildWhereClause(["deletedAt", "IS", null])
		assert.deepStrictEqual(result, {
			sql: "deletedAt IS NULL",
			params: [],
			fields: ["deletedAt"],
		})
	})

	await t.test("LIKE operator", () => {
		const result = buildWhereClause(["email", "LIKE", "%@example.com"])
		assert.deepStrictEqual(result, {
			sql: "email LIKE ?",
			params: ["%@example.com"],
			fields: ["email"],
		})
	})
})

test("buildWhereClause - IN Conditions", async (t) => {
	await t.test("IN with array of strings", () => {
		const result = buildWhereClause(["status", "IN", ["active", "pending"]])
		assert.deepStrictEqual(result, {
			sql: "status IN (?, ?)",
			params: ["active", "pending"],
			fields: ["status"],
		})
	})

	await t.test("NOT IN with array of numbers", () => {
		const result = buildWhereClause(["id", "NOT IN", [1, 2, 3]])
		assert.deepStrictEqual(result, {
			sql: "id NOT IN (?, ?, ?)",
			params: [1, 2, 3],
			fields: ["id"],
		})
	})

	await t.test("IN with empty array should throw", () => {
		assert.throws(() => buildWhereClause(["status", "IN", []]), NodeSqliteError)
	})
})

test("buildWhereClause - Complex Conditions", async (t) => {
	await t.test("simple AND condition", () => {
		const result = buildWhereClause([
			["age", ">=", 18],
			"AND",
			["active", "=", true],
		])
		assert.deepStrictEqual(result, {
			sql: "(age >= ? AND active = ?)",
			params: [18, 1],
			fields: ["age", "active"],
		})
	})

	await t.test("OR condition", () => {
		const result = buildWhereClause([
			["status", "=", "pending"],
			"OR",
			["status", "=", "active"],
		])
		assert.deepStrictEqual(result, {
			sql: "(status = ? OR status = ?)",
			params: ["pending", "active"],
			fields: ["status", "status"],
		})
	})

	await t.test("complex AND/OR combination", () => {
		const result = buildWhereClause([
			["age", ">=", 18],
			"AND",
			["status", "=", "active"],
			"OR",
			["role", "IN", ["admin", "moderator"]],
		])
		assert.deepStrictEqual(result, {
			sql: "(age >= ? AND status = ? OR role IN (?, ?))",
			params: [18, "active", "admin", "moderator"],
			fields: ["age", "status", "role"],
		})
	})
})

test("buildWhereClause - Type Handling", async (t) => {
	const queryKeys: Record<string, QueryKeyDef> = {
		age: { type: "INTEGER" },
		active: { type: "BOOLEAN" },
		score: { type: "REAL" },
		data: { type: "BLOB" },
	}

	await t.test("integer field conversion", () => {
		const result = buildWhereClause(["age", ">", 18], queryKeys)
		assert.deepStrictEqual(result, {
			sql: "age > ?",
			params: [18],
			fields: ["age"],
		})
	})

	await t.test("boolean field conversion", () => {
		const result = buildWhereClause(["active", "=", true], queryKeys)
		assert.deepStrictEqual(result, {
			sql: "active = ?",
			params: [1],
			fields: ["active"],
		})
	})

	await t.test("REAL field conversion", () => {
		const result = buildWhereClause(["score", ">=", 9.5], queryKeys)
		assert.deepStrictEqual(result, {
			sql: "score >= ?",
			params: [9.5],
			fields: ["score"],
		})
	})
})

test("buildWhereClause - Error Cases", async (t) => {
	await t.test("invalid operator should throw", () => {
		assert.throws(
			() => buildWhereClause(["age", "INVALID", 18] as WhereCondition<unknown>),
			NodeSqliteError
		)
	})

	await t.test("IS operator with non-null value should throw", () => {
		assert.throws(
			() => buildWhereClause(["status", "IS", "active"]),
			NodeSqliteError
		)
	})

	await t.test("IN operator with non-array value should throw", () => {
		assert.throws(
			() =>
				buildWhereClause(["status", "IN", "active"] as WhereCondition<unknown>),
			NodeSqliteError
		)
	})

	await t.test("invalid value type for INTEGER field", () => {
		const queryKeys = { age: { type: "INTEGER" as const } }
		assert.throws(
			() =>
				buildWhereClause(
					["age", "=", "not-a-number" as WhereCondition<unknown>],
					queryKeys
				),
			TypeError
		)
	})

	await t.test("invalid value type for REAL field", () => {
		const queryKeys = { score: { type: "REAL" as const } }
		assert.throws(
			() =>
				buildWhereClause(
					["score", "=", "not-a-number" as WhereCondition<unknown>],
					queryKeys
				),
			TypeError
		)
	})
})

test("buildWhereClause - Edge Cases", async (t) => {
	await t.test("very long IN clause", () => {
		const values = Array.from({ length: 1000 }, (_, i) => i)
		const result = buildWhereClause(["id", "IN", values])
		assert.strictEqual(result.params.length, 1000)
		assert.strictEqual(result.sql.split("?").length - 1, 1000)
	})

	await t.test("deeply nested conditions", () => {
		const result = buildWhereClause([
			["a", "=", 1],
			"AND",
			["b", "=", 2],
			"AND",
			["c", "=", 3],
			"AND",
			["d", "=", 4],
			"AND",
			["e", "=", 5],
		])
		assert.strictEqual(result.params.length, 5)
		assert.strictEqual(result.fields.length, 5)
	})

	await t.test("special characters in field names", () => {
		const result = buildWhereClause(['user"name', "=", "test"])
		assert.strictEqual(result.sql, 'user"name = ?')
	})
})

test("buildWhereClause - Type Coercion", async (t) => {
	const queryKeys: Record<string, QueryKeyDef> = {
		intField: { type: "INTEGER" },
		boolField: { type: "BOOLEAN" },
		realField: { type: "REAL" },
		textField: { type: "TEXT" },
	}

	await t.test("string to number coercion", () => {
		const result = buildWhereClause(["intField", "=", "123"], queryKeys)
		assert.deepStrictEqual(result.params, [123])
	})

	await t.test("number to boolean coercion", () => {
		const result = buildWhereClause(["boolField", "=", 1], queryKeys)
		assert.deepStrictEqual(result.params, [1])
	})

	await t.test("string to float coercion", () => {
		const result = buildWhereClause(["realField", "=", "123.45"], queryKeys)
		assert.deepStrictEqual(result.params, [123.45])
	})
})

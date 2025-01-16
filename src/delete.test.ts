// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { describe, it } from "node:test"
import assert from "node:assert/strict"
import { buildDeleteQuery, buildDeleteManyQuery } from "./delete.js"
import type { QueryKeys } from "./types.js"

interface TestEntity {
	id: number
	name: string
	age: number
	email: string
	active: boolean
}

const queryKeys: QueryKeys<TestEntity> = {
	id: { type: "INTEGER" },
	name: { type: "TEXT" },
	age: { type: "INTEGER" },
	email: { type: "TEXT" },
	active: { type: "BOOLEAN" },
}

describe("buildDeleteQuery", () => {
	it("builds a basic delete query without where clause", () => {
		const result = buildDeleteQuery("users", {})

		assert.deepEqual(result, {
			sql: "DELETE FROM users LIMIT 1",
			params: [],
		})
	})

	it("builds delete query with simple where clause", () => {
		const result = buildDeleteQuery<TestEntity>(
			"users",
			{ where: ["id", "=", 1] },
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE id = ? LIMIT 1",
			params: [1],
		})
	})

	it("builds delete query with IN operator", () => {
		const result = buildDeleteQuery<TestEntity>(
			"users",
			{ where: ["id", "IN", [1, 2, 3]] },
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE id IN (?, ?, ?) LIMIT 1",
			params: [1, 2, 3],
		})
	})

	it("builds delete query with complex where clause using AND", () => {
		const result = buildDeleteQuery<TestEntity>(
			"users",
			{
				where: [["age", ">", 25], "AND", ["active", "=", true]],
			},
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE (age > ? AND active = ?) LIMIT 1",
			params: [25, 1],
		})
	})

	it("builds delete query with complex where clause using OR", () => {
		const result = buildDeleteQuery<TestEntity>(
			"users",
			{
				where: [["email", "LIKE", "%test%"], "OR", ["name", "=", "John"]],
			},
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE (email LIKE ? OR name = ?) LIMIT 1",
			params: ["%test%", "John"],
		})
	})

	it("builds delete query with IS NULL operator", () => {
		const result = buildDeleteQuery<TestEntity>(
			"users",
			{ where: ["email", "IS", null] },
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE email IS NULL LIMIT 1",
			params: [],
		})
	})

	it("handles table names with special characters properly", () => {
		const result = buildDeleteQuery("user_data", {})

		assert.deepEqual(result, {
			sql: "DELETE FROM user_data LIMIT 1",
			params: [],
		})
	})
})

describe("buildDeleteManyQuery", () => {
	it("builds a basic delete many query without where clause", () => {
		const result = buildDeleteManyQuery("users", {})

		assert.deepEqual(result, {
			sql: "DELETE FROM users",
			params: [],
		})
	})

	it("builds delete many query with simple where clause", () => {
		const result = buildDeleteManyQuery<TestEntity>(
			"users",
			{ where: ["active", "=", false] },
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE active = ?",
			params: [0],
		})
	})

	it("builds delete many query with IN operator", () => {
		const result = buildDeleteManyQuery<TestEntity>(
			"users",
			{ where: ["id", "IN", [1, 2, 3, 4]] },
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE id IN (?, ?, ?, ?)",
			params: [1, 2, 3, 4],
		})
	})

	it("builds delete many query with complex where clause", () => {
		const result = buildDeleteManyQuery<TestEntity>(
			"users",
			{
				where: [
					["age", ">=", 18],
					"AND",
					["active", "=", true],
					"AND",
					["email", "LIKE", "%.org"],
				],
			},
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE (age >= ? AND active = ? AND email LIKE ?)",
			params: [18, 1, "%.org"],
		})
	})

	it("builds delete many query with NOT IN operator", () => {
		const result = buildDeleteManyQuery<TestEntity>(
			"users",
			{ where: ["id", "NOT IN", [5, 6, 7]] },
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE id NOT IN (?, ?, ?)",
			params: [5, 6, 7],
		})
	})

	it("builds delete many query with IS NOT NULL operator", () => {
		const result = buildDeleteManyQuery<TestEntity>(
			"users",
			{ where: ["email", "IS NOT", null] },
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE email IS NOT NULL",
			params: [],
		})
	})

	it("handles multiple OR conditions", () => {
		const result = buildDeleteManyQuery<TestEntity>(
			"users",
			{
				where: [
					["age", "<", 18],
					"OR",
					["age", ">", 65],
					"OR",
					["active", "=", false],
				],
			},
			queryKeys
		)

		assert.deepEqual(result, {
			sql: "DELETE FROM users WHERE (age < ? OR age > ? OR active = ?)",
			params: [18, 65, 0],
		})
	})
})

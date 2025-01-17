import test from "node:test"
import assert from "node:assert/strict"
import { buildFindQuery, type FindOptions } from "./find.js"
import type { QueryKeysSchema } from "./types.js"
import type { Where } from "./where.js"

interface TestEntity {
	name: string
	age: number
	active: boolean
}

test("buildFindQuery", async (t) => {
	const tableName = "test_table"

	// Define queryKeys for the test entity
	const queryKeys: QueryKeysSchema<TestEntity> = {
		name: { type: "TEXT" },
		age: { type: "INTEGER" },
		active: { type: "BOOLEAN" },
	}

	await t.test("should build a basic SELECT query", () => {
		const options: FindOptions<TestEntity> = {}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(result.sql, "SELECT * FROM test_table")
		assert.deepEqual(result.params, [])
	})

	await t.test("should add DISTINCT when specified", () => {
		const options: FindOptions<TestEntity> = {
			distinct: true,
		}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(result.sql, "SELECT DISTINCT * FROM test_table")
		assert.deepEqual(result.params, [])
	})

	await t.test("should add LIMIT and OFFSET when specified", () => {
		const options: FindOptions<TestEntity> = {
			limit: 10,
			offset: 5,
		}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(result.sql, "SELECT * FROM test_table LIMIT ? OFFSET ?")
		assert.deepEqual(result.params, [10, 5])
	})

	await t.test("should add ORDER BY when specified", () => {
		const options: FindOptions<TestEntity> = {
			orderBy: {
				age: "DESC",
				name: "ASC",
			},
		}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(
			result.sql,
			"SELECT * FROM test_table ORDER BY age DESC, name ASC"
		)
		assert.deepEqual(result.params, [])
	})

	await t.test("should combine multiple clauses correctly", () => {
		const options: FindOptions<TestEntity> = {
			distinct: true,
			limit: 10,
			offset: 5,
			orderBy: {
				age: "DESC",
			},
			where: ["age", ">", 21],
		}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(
			result.sql,
			"SELECT DISTINCT * FROM test_table WHERE age > ? ORDER BY age DESC LIMIT ? OFFSET ?"
		)
		assert.deepEqual(result.params, [21, 10, 5])
	})

	await t.test("should handle GROUP BY clauses", () => {
		const options: FindOptions<TestEntity> = {
			groupBy: ["age"],
		}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(result.sql, "SELECT * FROM test_table GROUP BY age")
		assert.deepEqual(result.params, [])
	})

	await t.test("should handle complex WHERE conditions", () => {
		const options: FindOptions<TestEntity> = {
			where: [["age", ">", 21], "AND", ["active", "=", true]],
		}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(
			result.sql,
			"SELECT * FROM test_table WHERE (age > ? AND active = ?)"
		)
		assert.deepEqual(result.params, [21, 1])
	})

	await t.test("should handle IN operator in WHERE clause", () => {
		const options: FindOptions<TestEntity> = {
			where: ["age", "IN", [21, 22, 23]],
		}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(result.sql, "SELECT * FROM test_table WHERE age IN (?, ?, ?)")
		assert.deepEqual(result.params, [21, 22, 23])
	})

	await t.test("should handle NOT IN operator in WHERE clause", () => {
		const options: FindOptions<TestEntity> = {
			where: ["age", "NOT IN", [21, 22, 23]],
		}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(
			result.sql,
			"SELECT * FROM test_table WHERE age NOT IN (?, ?, ?)"
		)
		assert.deepEqual(result.params, [21, 22, 23])
	})

	await t.test("should handle IS NULL in WHERE clause", () => {
		const options: FindOptions<TestEntity> = {
			where: ["name", "IS", null] as unknown as Where<unknown>,
		}
		const result = buildFindQuery(tableName, options, queryKeys)
		assert.equal(result.sql, "SELECT * FROM test_table WHERE name IS NULL")
		assert.deepEqual(result.params, [])
	})
})

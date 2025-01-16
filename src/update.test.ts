import { describe, it } from "node:test"
import assert from "node:assert"
import type { QueryKeys } from "./types.js"
import { buildUpdateManyQuery, buildUpdateQuery } from "./update.js"

describe("buildUpdateQuery", () => {
	// Test basic update with query keys
	it("builds basic update query with matching query keys", () => {
		interface TestEntity {
			name: string
			id: number
		}

		const queryKeys: QueryKeys<TestEntity> = {
			name: { type: "TEXT" },
			id: { type: "INTEGER" },
		}

		const entity: Partial<TestEntity> = { name: "test" }
		const { sql, params } = buildUpdateQuery<TestEntity>(
			"test_table",
			entity,
			{ where: ["id", "=", 1] },
			queryKeys,
			false
		)

		// Verify SQL structure
		assert.match(sql, /UPDATE test_table/)
		assert.match(sql, /SET name = \?, __lazy_data = \?/)
		assert.match(sql, /WHERE id = \?/)
		assert.match(sql, /RETURNING _id, __lazy_data/)

		// Verify params
		assert.equal(params.length, 2) // name param + WHERE clause parameter
		assert.equal(params[0], "test") // SET name = ?
		assert.equal(params[1], 1) // WHERE id = 1
	})

	// Test with all column types
	it("handles all column types correctly", () => {
		interface TestEntity {
			textField: string
			intField: number
			realField: number
			boolField: boolean
		}

		const queryKeys: QueryKeys<TestEntity> = {
			textField: { type: "TEXT" },
			intField: { type: "INTEGER" },
			realField: { type: "REAL" },
			boolField: { type: "BOOLEAN" },
		}

		const entity: Partial<TestEntity> = {
			textField: "test",
			intField: 42,
			realField: 3.14,
			boolField: true,
		}

		const { sql, params } = buildUpdateQuery<TestEntity>(
			"test_table",
			entity,
			{ where: ["intField", ">", 0] },
			queryKeys,
			false
		)

		// Check SQL structure
		assert.match(sql, /UPDATE test_table/)
		assert.match(
			sql,
			/SET textField = \?, intField = \?, realField = \?, boolField = \?, __lazy_data = \?/
		)
		assert.match(sql, /WHERE intField > \?/)

		// Check params (4 SET params + 1 WHERE param)
		assert.equal(params.length, 5)
		assert.equal(typeof params[0], "string") // textField
		assert.equal(typeof params[1], "number") // intField
		assert.equal(typeof params[2], "number") // realField
		assert.equal(typeof params[3], "number") // boolField (converted to 0/1)
		assert.equal(params[3], 1) // true -> 1
		assert.equal(params[4], 0) // WHERE param
	})

	// Test with timestamps
	it("includes timestamp fields when enabled", () => {
		interface TestEntity {
			name: string
			id: number
		}

		const queryKeys: QueryKeys<TestEntity> = {
			name: { type: "TEXT" },
			id: { type: "INTEGER" },
		}

		const entity: Partial<TestEntity> = { name: "test" }

		const { sql, params } = buildUpdateQuery<TestEntity>(
			"test_table",
			entity,
			{ where: ["id", "=", 1] },
			queryKeys,
			true
		)

		assert.match(sql, /updatedAt = CURRENT_TIMESTAMP/)
		assert.match(sql, /RETURNING _id, __lazy_data, createdAt, updatedAt/)
		assert.equal(params.length, 2) // SET param + WHERE param
	})

	// Test with nullable fields
	it("handles nullable fields correctly", () => {
		interface TestEntityNullable {
			required: string
			optional: string | null
		}

		const queryKeys: QueryKeys<TestEntityNullable> = {
			required: { type: "TEXT" },
			optional: { type: "TEXT", nullable: true },
		}

		const entity: Partial<TestEntityNullable> = {
			optional: null,
		}

		const { sql, params } = buildUpdateQuery<TestEntityNullable>(
			"test_table",
			entity,
			{ where: ["required", "=", "value"] },
			queryKeys,
			false
		)

		assert.match(sql, /optional = \?/)
		assert.equal(params[0], null)
		assert.equal(params[1], "value") // WHERE param
	})

	// Test with ignored fields
	it("ignores _id and createdAt fields", () => {
		interface TestEntityWithIgnored {
			name: string
			status: string
			_id?: number
			createdAt?: string
			updatedAt?: string
		}

		const queryKeys: QueryKeys<TestEntityWithIgnored> = {
			name: { type: "TEXT" },
			status: { type: "TEXT" },
		}

		const entity: Partial<TestEntityWithIgnored> = {
			name: "test",
			_id: 1,
			createdAt: "now",
			updatedAt: "now",
		}

		const { sql, params } = buildUpdateQuery<TestEntityWithIgnored>(
			"test_table",
			entity,
			{ where: ["status", "=", "active"] },
			queryKeys,
			false
		)

		// Check that ignored fields are not in SET clause
		const setPart = sql.split("WHERE")[0]
		assert.ok(!setPart.includes("_id ="))
		assert.ok(!setPart.includes("createdAt ="))

		// Should have: 'test' in SET params + 'active' in WHERE params
		assert.deepEqual(
			params,
			["test", "active"],
			`Expected ["test", "active"] but got ${JSON.stringify(params)}`
		)
	})

	// Test complex WHERE conditions
	it("handles complex WHERE conditions", () => {
		interface TestEntityComplex {
			name: string
			age: number
			status: string
		}

		const queryKeys: QueryKeys<TestEntityComplex> = {
			name: { type: "TEXT" },
			age: { type: "INTEGER" },
			status: { type: "TEXT" },
		}

		const entity: Partial<TestEntityComplex> = { name: "test" }

		const { sql, params } = buildUpdateQuery<TestEntityComplex>(
			"test_table",
			entity,
			{
				where: [["age", ">", 18], "AND", ["status", "=", "active"]],
			},
			queryKeys,
			false
		)

		// Test complete structure
		const expectedSqlPattern = new RegExp(
			"UPDATE test_table\\s+" +
				"SET name = \\?, __lazy_data = \\?\\s+" +
				"WHERE \\(?age > \\? AND status = \\?\\)?\\s+" +
				"RETURNING _id, __lazy_data"
		)

		assert.match(
			sql.replace(/\n/g, " ").trim(),
			expectedSqlPattern,
			`SQL doesn't match expected pattern: ${sql}`
		)

		// Should have name param + two WHERE params
		assert.deepEqual(
			params,
			["test", 18, "active"],
			`Expected ["test", 18, "active"] but got ${JSON.stringify(params)}`
		)
	})
})

describe("buildUpdateManyQuery", () => {
	// Test basic update with simple fields
	it("builds basic update query", () => {
		interface TestEntity {
			name: string
			age: number
			active: boolean
		}

		const updates: Partial<TestEntity> = {
			name: "test",
			age: 25,
		}

		const queryKeys: QueryKeys<TestEntity> = {
			name: { type: "TEXT" },
			age: { type: "INTEGER" },
			active: { type: "BOOLEAN" },
		}

		const { sql, params } = buildUpdateManyQuery<TestEntity>(
			"test_table",
			updates,
			{ where: ["active", "=", true] },
			queryKeys
		)

		assert.equal(
			sql,
			"UPDATE test_table SET name = ?, age = ? WHERE active = ?"
		)
		assert.deepStrictEqual(params, ["test", 25, 1])
	})

	// Test with timestamps enabled
	it("includes updatedAt when timestamps enabled", () => {
		interface TestEntity {
			name: string
		}

		const updates: Partial<TestEntity> = {
			name: "test",
		}

		const queryKeys: QueryKeys<TestEntity> = {
			name: { type: "TEXT" },
		}

		const { sql, params } = buildUpdateManyQuery<TestEntity>(
			"test_table",
			updates,
			{ where: ["name", "=", "old"] },
			queryKeys,
			true
		)

		assert.equal(
			sql,
			"UPDATE test_table SET name = ?, updatedAt = CURRENT_TIMESTAMP WHERE name = ?"
		)
		assert.deepStrictEqual(params, ["test", "old"])
	})

	// Test with multiple WHERE conditions
	it("handles complex WHERE conditions", () => {
		interface TestEntity {
			status: string
			age: number
			name: string
		}

		const updates: Partial<TestEntity> = {
			status: "active",
		}

		const queryKeys: QueryKeys<TestEntity> = {
			status: { type: "TEXT" },
			age: { type: "INTEGER" },
			name: { type: "TEXT" },
		}

		const { sql, params } = buildUpdateManyQuery<TestEntity>(
			"test_table",
			updates,
			{ where: [["age", ">", 18], "AND", ["name", "LIKE", "John%"]] },
			queryKeys
		)

		assert.equal(
			sql,
			"UPDATE test_table SET status = ? WHERE (age > ? AND name LIKE ?)"
		)
		assert.deepStrictEqual(params, ["active", 18, "John%"])
	})

	// Test with no WHERE clause
	it("handles updates with no WHERE clause", () => {
		interface TestEntity {
			name: string
		}

		const updates: Partial<TestEntity> = {
			name: "test",
		}

		const queryKeys: QueryKeys<TestEntity> = {
			name: { type: "TEXT" },
		}

		const { sql, params } = buildUpdateManyQuery<TestEntity>(
			"test_table",
			updates,
			{},
			queryKeys
		)

		assert.equal(sql, "UPDATE test_table SET name = ?")
		assert.deepStrictEqual(params, ["test"])
	})

	// Test with different data types
	it("handles different data types correctly", () => {
		interface TestEntity {
			textField: string
			numberField: number
			boolField: boolean
			nullField: string | null
		}

		const updates: Partial<TestEntity> = {
			textField: "test",
			numberField: 123,
			boolField: true,
			nullField: null,
		}

		const queryKeys: QueryKeys<TestEntity> = {
			textField: { type: "TEXT" },
			numberField: { type: "INTEGER" },
			boolField: { type: "BOOLEAN" },
			nullField: { type: "TEXT", nullable: true },
		}

		const { sql, params } = buildUpdateManyQuery<TestEntity>(
			"test_table",
			updates,
			{},
			queryKeys
		)

		assert.equal(
			sql,
			"UPDATE test_table SET textField = ?, numberField = ?, boolField = ?, nullField = ?"
		)
		assert.deepStrictEqual(params, ["test", 123, true, null])
	})

	// Test empty updates object with timestamps
	it("handles empty updates object with timestamps", () => {
		interface TestEntity {
			name: string
		}

		const updates: Partial<TestEntity> = {}

		const queryKeys: QueryKeys<TestEntity> = {
			name: { type: "TEXT" },
		}

		const { sql, params } = buildUpdateManyQuery<TestEntity>(
			"test_table",
			updates,
			{ where: ["name", "=", "test"] },
			queryKeys,
			true
		)

		assert.equal(
			sql,
			"UPDATE test_table SET updatedAt = CURRENT_TIMESTAMP WHERE name = ?"
		)
		assert.deepStrictEqual(params, ["test"])
	})

	// Test ignoring system fields
	it("ignores system fields (_id, createdAt, updatedAt)", () => {
		interface TestEntity {
			name: string
			_id?: number
			createdAt?: string
			updatedAt?: string
		}

		const queryKeys: QueryKeys<TestEntity> = {
			name: { type: "TEXT" },
			_id: { type: "INTEGER" },
			createdAt: { type: "TEXT" },
			updatedAt: { type: "TEXT" },
		}

		const updates: Partial<TestEntity> = {
			name: "test",
			_id: 1,
			createdAt: "now",
			updatedAt: "now",
		}

		const { sql, params } = buildUpdateManyQuery<TestEntity>(
			"test_table",
			updates,
			{},
			queryKeys
		)

		assert.equal(sql, "UPDATE test_table SET name = ?")
		assert.deepStrictEqual(params, ["test"])
	})
})

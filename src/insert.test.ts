// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { describe, it } from "node:test"
import { buildInsertManyQuery } from "./sql.js"
import assert from "node:assert"
import type { QueryKeys } from "./types.js"

describe("buildInsertManyQuery", () => {
	// Test empty input
	it("handles empty entity array", () => {
		const { sql, values } = buildInsertManyQuery("test_table", [], {}, false)
		assert.equal(sql, "")
		assert.equal(values.length, 0)
	})

	// Test basic insert with no queryKeys
	it("builds basic insert query with no queryKeys", () => {
		const entities = [{ name: "test1" }, { name: "test2" }]
		const { sql, values } = buildInsertManyQuery(
			"test_table",
			entities,
			undefined,
			false
		)

		// More specific SQL assertions
		assert.match(sql, /INSERT INTO test_table/)
		assert.match(sql, /__lazy_data/)
		assert.match(sql, /RETURNING _id, __lazy_data/)

		assert.equal(values.length, 2)
		for (const valueSet of values) {
			assert.equal(valueSet.length, 1) // Only __lazy_data column
			assert(valueSet[0] instanceof Uint8Array)
		}
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

		const entities: TestEntity[] = [
			{
				textField: "test",
				intField: 42,
				realField: 3.14,
				boolField: true,
			},
			{
				textField: "test2",
				intField: 43,
				realField: Math.E,
				boolField: false,
			},
		]

		const { sql, values } = buildInsertManyQuery(
			"test_table",
			entities,
			queryKeys,
			false
		)

		// Check SQL structure
		assert.match(sql, /INSERT INTO test_table/)
		assert.match(sql, /textField.*intField.*realField.*boolField/)

		// Check values
		assert.equal(values.length, 2)
		for (const valueSet of values) {
			assert.equal(valueSet.length, 5) // 4 fields + __lazy_data
			assert.equal(typeof valueSet[0], "string")
			assert.equal(typeof valueSet[1], "number")
			assert.equal(typeof valueSet[2], "number")
			assert.equal(typeof valueSet[3], "number") // Boolean converted to 0/1
			assert(valueSet[4] instanceof Uint8Array)
		}
	})

	// Test with timestamps
	it("includes RETURNING clause with timestamps", () => {
		const entities = [{ name: "test" }]
		const queryKeys: QueryKeys<{ name: string }> = {
			name: { type: "TEXT" },
		}

		const { sql } = buildInsertManyQuery(
			"test_table",
			entities,
			queryKeys,
			true
		)

		assert.match(sql, /RETURNING.*createdAt.*updatedAt/)
	})

	// Test with nullable fields
	it("handles nullable fields correctly", () => {
		interface TestEntityNullable {
			required: string
			optional?: string
		}

		const queryKeys: QueryKeys<TestEntityNullable> = {
			required: { type: "TEXT" },
			optional: { type: "TEXT", nullable: true },
		}

		const entities: TestEntityNullable[] = [
			{ required: "test1" },
			{ required: "test2", optional: "opt" },
		]

		const { sql, values } = buildInsertManyQuery(
			"test_table",
			entities,
			queryKeys,
			false
		)

		assert.match(sql, /INSERT INTO test_table/)
		assert.equal(values.length, 2)
		// Second entity should have both values
		assert.equal(values[1].length - 1, 2) // -1 for __lazy_data
	})

	// Test with unique constraints
	it("handles unique constraints in queryKeys", () => {
		interface TestEntityUnique {
			uniqueField: string
		}

		const queryKeys: QueryKeys<TestEntityUnique> = {
			uniqueField: { type: "TEXT", unique: true },
		}

		const entities: TestEntityUnique[] = [
			{ uniqueField: "unique1" },
			{ uniqueField: "unique2" },
		]

		const { sql, values } = buildInsertManyQuery(
			"test_table",
			entities,
			queryKeys,
			false
		)

		assert.match(sql, /INSERT INTO test_table/)
		assert.equal(values.length, 2)
		for (const valueSet of values) {
			assert.equal(typeof valueSet[0], "string")
		}
	})

	// Test with default values
	it("handles default values in queryKeys", () => {
		interface TestEntityDefault {
			fieldWithDefault: string
		}

		const queryKeys: QueryKeys<TestEntityDefault> = {
			fieldWithDefault: { type: "TEXT", default: "default_value" },
		}

		const entities: TestEntityDefault[] = [
			{ fieldWithDefault: "custom" },
			{ fieldWithDefault: "custom2" },
		]

		const { sql, values } = buildInsertManyQuery(
			"test_table",
			entities,
			queryKeys,
			false
		)

		assert.match(sql, /INSERT INTO test_table/)
		assert.equal(values.length, 2)
		for (const valueSet of values) {
			assert.equal(typeof valueSet[0], "string")
		}
	})

	// Test with ignored fields
	it("ignores _id, createdAt, and updatedAt fields", () => {
		interface TestEntityWithIgnored {
			name: string
			_id?: number
			createdAt?: string
			updatedAt?: string
		}

		const queryKeys: QueryKeys<TestEntityWithIgnored> = {
			name: { type: "TEXT" },
			_id: { type: "INTEGER" },
			createdAt: { type: "TEXT" },
			updatedAt: { type: "TEXT" },
		}

		const entities: TestEntityWithIgnored[] = [
			{ name: "test", _id: 1, createdAt: "now", updatedAt: "now" },
		]

		const { sql, values } = buildInsertManyQuery(
			"test_table",
			entities,
			queryKeys,
			false
		)

		console.log(`the sql is ${sql}, the values is ${values}`)

		// Test SQL structure
		assert.match(sql, /INSERT INTO test_table/)
		assert.ok(!sql.includes("createdAt"))
		assert.ok(!sql.includes("updatedAt"))

		// Test values array structure
		assert.equal(values.length, 1) // One entity
		assert.equal(values[0].length, 2) // name + __lazy_data
		assert.equal(values[0][0], "test") // First value should be the name
		assert(values[0][1] instanceof Uint8Array) // Second value should be __lazy_data placeholder
	})
})

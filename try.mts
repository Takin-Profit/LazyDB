import { buildCreateTableSQL } from "./src/sql"
import type { QueryKeys } from "./src/types"
// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Test script to verify buildCreateTableSQL behavior
function testBuildCreateTableSQL() {
	for (let i = 0; i < 20; i++) {
		try {
			const invalidQueryKeys = {
				field: { type: `INVALID_TYPE${i}` },
			}

			const result = buildCreateTableSQL(
				`test_table_${i}`,
				invalidQueryKeys as QueryKeys<Record<string, string>>
			)
			console.log(`Iteration ${i}: Unexpectedly succeeded`)
		} catch (err) {
			console.log(`Iteration ${i}: Caught error:`, err.message)
		}
	}
	console.log("All iterations complete")
}

testBuildCreateTableSQL()

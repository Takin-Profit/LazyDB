// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import type { GroupByTuples } from "./group-by.js"
import type { RepositoryOptions } from "./types.js"
import {
	array,
	bool,
	literal,
	num,
	object,
	optional,
	record,
	string,
	union,
} from "./utils.js"
import { Where } from "./where.js"

// Update the FindOptionsSchema
export const FindOptions = object({
	where: optional(Where),
	limit: optional(num()),
	offset: optional(num()),
	orderBy: optional(record(string(), union([literal("ASC"), literal("DESC")]))),
	distinct: optional(bool()),
	groupBy: optional(array(string())),
})

export type FindOptions<T extends { [key: string]: unknown }> = {
	where?: Where<T>
	limit?: number
	offset?: number
	orderBy?: T extends {
		[K in keyof Required<
			NonNullable<RepositoryOptions<T>["queryKeys"]>
		>]: unknown
	}
		? Partial<
				Record<
					keyof NonNullable<RepositoryOptions<T>["queryKeys"]>,
					"ASC" | "DESC"
				>
			>
		: never
	distinct?: boolean
	groupBy?: GroupByTuples<
		keyof NonNullable<RepositoryOptions<T>["queryKeys"]> & string
	>
}

export function isGroupByArray<T extends { [key: string]: unknown }>(
	groupBy: FindOptions<T>["groupBy"]
): groupBy is Extract<FindOptions<T>["groupBy"], string[]> {
	return (
		Array.isArray(groupBy) &&
		(groupBy as Array<T>).length > 0 &&
		(groupBy as Array<T>).every((key) => typeof key === "string")
	)
}

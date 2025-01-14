// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { Type } from "@sinclair/typebox"

const schema = Type.Object(
	{
		foo: Type.String(),
		bar: Type.Number(),
	},
	{
		$id: "Schema",
		title: "Schema",
		queryColumns: {
			name: "gary",
		},
	}
)

console.log(schema)

// Copyright 2024 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

export {
	Document,
	Result,
	DatabaseError,
	Filter,
	FindOptions,
	SafeDatabaseOptions,
	SafeRootDatabaseOptionsWithPath,
	OperationStats,
} from "./types.js"

export { Collection } from "./collection.js"
export { Database } from "./database.js"
export { isError } from "./collection.js"

// Example usage:
/*
import { Database, Document, isError } from './index'

type User = Document<{
  name: string
  email: string
  age: number
}>

const db = new Database({ path: './data' })
const users = db.collection<User>('users')

async function example() {
  // Insert
  const userResult = await users.insert({
    name: 'John',
    email: 'john@example.com',
    age: 30
  })

  if (isError(userResult)) {
    console.error('Failed to insert:', userResult.error)
    return
  }

  // Find
  const found = await users.find({
    age: { $gt: 25 },
    name: { $regex: /^J/ }
  })

  if (isError(found)) {
    console.error('Failed to find:', found.error)
    return
  }

  // Results are properly typed
  found.forEach(user => console.log(user.name))
}
     */

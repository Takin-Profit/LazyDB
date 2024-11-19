// Copyright 2024 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Export types
export type {
	Document,
	FindOptions,
	SafeDatabaseOptions,
	SafeRootDatabaseOptionsWithPath,
	OperationStats,
	IdGenerator,
} from "./types.js"

// Export classes
export { Collection } from "./collection.js"
export { Database } from "./database.js"

// Export errors
export {
	DatabaseError,
	ValidationError,
	ConstraintError,
	TransactionError,
	IOError,
	CorruptionError,
	UpdateFailedError,
	OperationError,
	UnknownError,
} from "./errors.js"

// Example usage:
/*
import { Database, Document, NotFoundError } from './index'

type User = Document<{
  name: string
  email: string
  age: number
}>

const db = new Database('./data')
const users = db.collection<User>('users')

async function example() {
  try {
    // Insert
    const user = await users.insert({
      name: 'John',
      email: 'john@example.com',
      age: 30,
    })
    console.log('Inserted user:', user)

    // Find
    const found = users.find({
      where: (entry) => entry.value.age > 25 && /^J/.test(entry.value.name),
    })

    found.asArray.forEach((user) => console.log(user.name))
  } catch (error) {
    if (error instanceof NotFoundError) {
      console.error('Document not found:', error.message)
    } else {
      console.error('Operation failed:', error)
    }
  }
}
*/

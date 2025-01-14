// Copyright 2024 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Export types
export * from "./types.js"

// Example usage:
/**
import { Database, Entity, NotFoundError } from './index'

type User = Entity<{
  name: string
  email: string
  age: number
}>

const db = new Database('./data')
const users = db.repository<User>('users')

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
      console.error('Entity not found:', error.message)
    } else {
      console.error('Operation failed:', error)
    }
  }
}
**/

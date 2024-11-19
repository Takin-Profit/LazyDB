# LazyDB

LazyDB is a high-performance, type-safe, and easy-to-use database library built on top of LMDB. It provides a high-level interface for managing collections of documents with support for transactions, validation, and various error handling mechanisms.

## Overview

LazyDB is designed to be a lightweight and efficient database solution for Node.js applications. It leverages the power of LMDB to provide fast and reliable data storage while offering a simple and intuitive API for developers.

## Features

- **Type-safe collections**: Define and enforce document schemas using TypeScript.
- **Transactions**: Perform multiple operations atomically with transaction support.
- **Validation**: Ensure data integrity with built-in validation mechanisms.
- **Error handling**: Comprehensive error handling with custom error types.
- **Event-driven**: React to database events with an event-driven architecture.
- **Backup and restore**: Create and restore database backups easily.

## Getting Started

### Installation

To install LazyDB, use your preferred package manager:

```sh
npm install @takinprofit/lazydb
# or
pnpm add @takinprofit/lazydb
```

### Basic Usage

Here is a simple example to get you started with LazyDB:

```typescript
import { Database, Document, NotFoundError } from '@takinprofit/lazydb'

type User = Document<{
  name: string
  email: string
  age: number
}>

const db = new Database('./data')
const users = db.collection<User>('users')

async function example() {
  try {
    // Insert a new user
    const user = await users.insert({
      name: 'John Doe',
      email: 'john@example.com',
      age: 30,
    })
    console.log('Inserted user:', user)

    // Find users
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

example()
```

### Advanced Usage

#### Transactions

Perform multiple operations atomically using transactions:

```typescript
async function transactionalExample() {
  await users.transaction(async () => {
    const user1 = await users.insert({ name: 'Alice', email: 'alice@example.com', age: 25 })
    const user2 = await users.insert({ name: 'Bob', email: 'bob@example.com', age: 28 })
    console.log('Inserted users:', user1, user2)
  })
}

transactionalExample()
```

#### Error Handling

LazyDB provides custom error types for better error handling:

```typescript
import { ValidationError, ConstraintError, TransactionError } from '@takinprofit/lazydb'

async function errorHandlingExample() {
  try {
    await users.insert({ name: '', email: 'invalid-email', age: -1 })
  } catch (error) {
    if (error instanceof ValidationError) {
      console.error('Validation error:', error.message)
    } else if (error instanceof ConstraintError) {
      console.error('Constraint error:', error.message)
    } else if (error instanceof TransactionError) {
      console.error('Transaction error:', error.message)
    } else {
      console.error('Unknown error:', error)
    }
  }
}

errorHandlingExample()
```

#### Backup and Restore

Create and restore database backups easily:

```typescript
async function backupExample() {
  await db.backup('./backup-path')
  console.log('Backup completed successfully')
}

backupExample()
```

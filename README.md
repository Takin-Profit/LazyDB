<p align="center">
  <img  src="logo.png" alt="LazyDB Logo" width="256" height="256" />
</p>

# LazyDB

LazyDB is a high-performance, type-safe document database built on top of LMDB (Lightning Memory-Mapped Database). It provides an intuitive, functional API with strong TypeScript support, ACID-compliant transactions, and robust error handling.

[![npm version](https://badge.fury.io/js/@takinprofit%2Flazydb.svg)](https://www.npmjs.com/package/@takinprofit/lazydb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Type-Safe Collections**: Full TypeScript support with type inference for documents and queries
- **ACID Transactions**: Guaranteed data consistency with atomic operations
- **High Performance**: Built on LMDB for lightning-fast read and write operations
- **Event-Driven Architecture**: React to database operations with typed event emitters
- **Flexible Querying**: Rich query API with support for filtering, mapping, and pagination
- **Robust Error Handling**: Comprehensive error types for precise error management
- **Advanced Features**: Document versioning, backup/restore, and compression support
- **Memory Efficient**: Uses memory-mapped files for optimal memory usage
- **Multi-Process Safe**: Safe for use across multiple Node.js processes

## Table of Contents

- [LazyDB](#lazydb)
  - [Features](#features)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
  - [Database Operations](#database-operations)
    - [Initialization](#initialization)
    - [Collection Management](#collection-management)
  - [Collections](#collections)
    - [Basic Operations](#basic-operations)
    - [Upsert Operations](#upsert-operations)
    - [RangeIterable Operations](#rangeiterable-operations)
    - [Advanced Query Patterns](#advanced-query-patterns)
  - [Transactions](#transactions)
  - [Events](#events)
    - [Database Events](#database-events)
    - [Collection Events](#collection-events)
  - [Error Handling](#error-handling)
  - [Advanced Features](#advanced-features)
    - [Custom ID Generation](#custom-id-generation)
    - [Compression Settings](#compression-settings)
    - [Database Options](#database-options)
  - [Testing](#testing)
  - [Contributing](#contributing)
    - [Development Setup](#development-setup)
    - [Project Structure](#project-structure)
    - [Available Scripts](#available-scripts)
    - [Code Quality](#code-quality)
    - [Submitting Changes](#submitting-changes)
    - [Package Distribution](#package-distribution)
    - [Issues and Bugs](#issues-and-bugs)
  - [API Reference](#api-reference)
  - [License](#license)
  - [Acknowledgments](#acknowledgments)

## Installation

```bash
npm install @takinprofit/lazydb

# or with yarn
yarn add @takinprofit/lazydb

# or with pnpm
pnpm add @takinprofit/lazydb
```

## Quick Start

```typescript
import { Database, Document } from '@takinprofit/lazydb'

// Define your document type
interface User extends Document {
  name: string
  email: string
  age: number
  active: boolean
}

// Initialize the database
const db = new Database('./my-database', {
  compression: true,
  maxCollections: 10
})

// Create a collection
const users = db.collection<User>('users')

// Insert a document
const user = await users.insert({
  name: 'John Doe',
  email: 'john@example.com',
  age: 30,
  active: true
})

// Find documents
const activeUsers = users.find({
  where: user => user.active && user.age >= 18
})

// Use the results
for (const user of activeUsers) {
  console.log(user.name)
}

// Clean up
await db.close()
```

## Database Operations

### Initialization

```typescript
import { Database } from '@takinprofit/lazydb'

const db = new Database('./db-path', {
  compression: true,          // Enable LZ4 compression
  maxCollections: 10,         // Maximum number of collections
  pageSize: 8192,            // Database page size
  overlappingSync: true,     // Enable overlapping sync for better performance
  logger: console.log        // Optional logging function
})
```

### Collection Management

```typescript
// Create/get a collection
const users = db.collection<User>('users')

// Clear a collection
await db.clearCollection('users')

// Drop a collection
await db.dropCollection('users')

// Clear all collections
await db.clearAll()

// Create a backup
await db.backup('./backup-path', true) // Second parameter enables compaction

// Close the database
await db.close()
```

## Collections

Collections provide type-safe access to groups of related documents.

### Basic Operations

```typescript
// Insert a single document
const user = await users.insert({
  name: 'Jane Smith',
  email: 'jane@example.com',
  age: 25,
  active: true
})

// Insert multiple documents
const newUsers = await users.insertMany([
  { name: 'User 1', email: 'user1@example.com', age: 20, active: true },
  { name: 'User 2', email: 'user2@example.com', age: 30, active: false }
])

// Update a document
const updated = await users.updateOne(
  { where: user => user._id === 'some-id' },
  { age: 26, active: false }
)

// Update multiple documents
const updateCount = await users.updateMany(
  { where: user => user.age < 18 },
  { active: false }
)

// Remove a document
const removed = await users.removeOne({
  where: user => user.email === 'jane@example.com'
})

// Remove multiple documents
const removedCount = await users.removeMany({
  where: user => !user.active
})
```

### Upsert Operations

```typescript
// Upsert a single document
const upserted = await users.upsert(
  { where: user => user.email === 'john@example.com' },
  { name: 'John Doe', email: 'john@example.com', age: 30, active: true }
)

// Upsert multiple documents
const upsertedUsers = await users.upsertMany([
  {
    where: user => user.email === 'user1@example.com',
    doc: { name: 'User 1', email: 'user1@example.com', age: 25, active: true }
  },
  {
    where: user => user.email === 'user2@example.com',
    doc: { name: 'User 2', email: 'user2@example.com', age: 35, active: true }
  }
])

## Querying

LazyDB provides a powerful querying API through the `find()` method, which returns a `RangeIterable`. This allows for efficient, lazy evaluation of query results with support for synchronous and asynchronous transformations.

### Basic Queries

```typescript
// Find all active users
const activeUsers = users.find({
  where: user => user.active
})

// Find with multiple conditions
const qualifiedUsers = users.find({
  where: user => user.age >= 18 && user.active && user.email.includes('@example.com')
})

// Using snapshot option for long-running queries
const largeDataset = users.find({
  where: user => user.active,
  snapshot: false  // Allows LMDB to collect freed space during iteration
})
```

### RangeIterable Operations

The `RangeIterable` class provides several methods for working with query results. These operations are lazy, meaning they're only executed when the results are actually consumed.

```typescript
// Basic mapping and filtering
const activeUserNames = users.find()
  .filter(user => user.active)
  .map(user => user.name)
  .asArray

// Asynchronous mapping
const userDetailsAsync = users.find()
  .map(async user => {
    const details = await fetchExternalDetails(user.id)
    return {
      ...user,
      details
    }
  })

// Process async results
for await (const user of userDetailsAsync) {
  console.log(user.details)
}

// Asynchronous filtering
const verifiedUsers = users.find()
  .filter(async user => {
    const isValid = await validateUserExternally(user)
    return isValid
  })

// Combine sync and async operations
const processedUsers = users.find()
  .filter(user => user.active)
  .map(async user => {
    const enriched = await enrichUserData(user)
    return enriched
  })
  .filter(async user => {
    const valid = await validateEnrichedUser(user)
    return valid
  })

// Error handling with mapError
const safeUserData = users.find()
  .map(user => {
    if (!user.email) throw new Error('Missing email')
    if (!user.age) throw new Error('Missing age')
    return {
      email: user.email.toLowerCase(),
      age: user.age
    }
  })
  .mapError(error => {
    console.error('Processing error:', error.message)
    // Return a default value to continue iteration
    return { email: 'invalid@example.com', age: 0 }
  })

// Terminate iteration on specific errors
const validatedUsers = users.find()
  .map(user => {
    if (!user.email) throw new Error('MISSING_EMAIL')
    if (!user.age) throw new Error('MISSING_AGE')
    return user
  })
  .mapError(error => {
    if (error.message === 'MISSING_EMAIL') {
      throw error // Terminate iteration
    }
    // Continue iteration with default for other errors
    return { email: 'unknown@example.com', age: 0 }
  })

// Pagination using slice
const pageSize = 10
const pageNumber = 1
const pagedUsers = users.find()
  .filter(user => user.active)
  .slice(pageNumber * pageSize, (pageNumber + 1) * pageSize)
  .asArray

// Chaining with error handling
const processedData = users.find()
  .filter(async user => await isEligible(user))
  .map(async user => {
    const enriched = await enrichUserData(user)
    if (!enriched) throw new Error('Enrichment failed')
    return enriched
  })
  .mapError(error => {
    logger.error('Processing failed:', error)
    return null
  })
  .filter(result => result !== null)
  .asArray
```

### Advanced Query Patterns

```typescript
// Combining multiple conditions with async validation
const complexQuery = users.find()
  .filter(user => user.age >= 18 && user.status === 'active')
  .map(async user => {
    const [verificationStatus, externalData] = await Promise.all([
      verifyUser(user),
      fetchExternalData(user.id)
    ])

    return {
      ...user,
      verified: verificationStatus,
      externalData
    }
  })
  .filter(user => user.verified)
  .mapError(error => {
    logger.error('Query processing error:', error)
    return null
  })

// Process results in chunks
const batchSize = 100
for await (const user of complexQuery) {
  if (user) {
    await processBatch(user)
  }
}
```

## Transactions

LazyDB supports ACID-compliant transactions for atomic operations. Note that transaction callbacks should not contain async operations.

```typescript
// Simple transaction
const result = await users.transaction(() => {
  const user = users.get('some-id')
  if (user && user.active) {
    users.putSync(user._id, { ...user, lastLogin: new Date() })
    return true
  }
  return false
})

// Conditional operations
await users.ifNoExists('unique-email', () => {
  users.putSync('unique-email', {
    name: 'New User',
    email: 'unique@example.com',
    age: 25,
    active: true
  })
})
```

## Events

LazyDB provides an event system for monitoring database operations:

### Database Events

```typescript
// Database-level events
db.on('collection.created', ({ name }) => {
  console.log(`Collection ${name} was created`)
})

db.on('collection.cleared', ({ name }) => {
  console.log(`Collection ${name} was cleared`)
})

db.on('collection.dropped', ({ name }) => {
  console.log(`Collection ${name} was dropped`)
})

db.on('database.cleared', () => {
  console.log('All collections were cleared')
})

db.on('database.closed', () => {
  console.log('Database was closed')
})

// Backup events
db.on('backup.started', ({ path, compact }) => {
  console.log(`Backup started to ${path}`)
})

db.on('backup.completed', ({ path, compact }) => {
  console.log(`Backup completed to ${path}`)
})

db.on('backup.failed', ({ path, error }) => {
  console.error(`Backup failed: ${error.message}`)
})
```

### Collection Events

```typescript
// Document operations
users.on('document.inserted', ({ document }) => {
  console.log('New document inserted:', document)
})

users.on('document.updated', ({ old, new: updated }) => {
  console.log('Document updated from:', old, 'to:', updated)
})

users.on('document.removed', ({ document }) => {
  console.log('Document removed:', document)
})

// Bulk operations
users.on('documents.inserted', ({ documents }) => {
  console.log(`${documents.length} documents inserted`)
})

users.on('documents.updated', ({ count }) => {
  console.log(`${count} documents updated`)
})

users.on('documents.removed', ({ count }) => {
  console.log(`${count} documents removed`)
})

// Upsert operations
users.on('document.upserted', ({ document, wasInsert }) => {
  console.log(`Document ${wasInsert ? 'inserted' : 'updated'}:`, document)
})

users.on('documents.upserted', ({ documents, insertCount, updateCount }) => {
  console.log(`Upserted ${documents.length} documents (${insertCount} inserts, ${updateCount} updates)`)
})
```

## Error Handling

LazyDB provides specific error types for different scenarios:

```typescript
import {
  DatabaseError,
  ValidationError,
  ConstraintError,
  TransactionError,
  NotFoundError,
  IOError,
  CorruptionError,
  UpdateFailedError,
  OperationError,
  UnknownError
} from '@takinprofit/lazydb'

try {
  await users.insert({ /* invalid document */ })
} catch (error) {
  if (error instanceof ValidationError) {
    console.error('Validation failed:', error.message)
    console.log('Fields:', error.fields)
  } else if (error instanceof ConstraintError) {
    console.error('Constraint violation:', error.constraint)
  } else if (error instanceof TransactionError) {
    console.error('Transaction failed:', error.message)
  } else if (error instanceof NotFoundError) {
    console.error('Document not found:', error.message)
  } else if (error instanceof DatabaseError) {
    // Base class for all database errors
    console.error('Database error:', error.type, error.message)
  }
}
```

## Advanced Features

### Custom ID Generation

```typescript
const db = new Database('./db-path', {
  idGenerator: () => `custom-${Date.now()}-${Math.random()}`
})
```

### Compression Settings

```typescript
const db = new Database('./db-path', {
  compression: {
    threshold: 1000,    // Compress entries larger than 1000 bytes
    dictionary: Buffer.from('custom-dictionary')
  }
})
```

### Database Options

```typescript
const db = new Database('./db-path', {
  compression: true,
  pageSize: 8192,
  overlappingSync: true,
  maxCollections: 10,
  commitDelay: 0,
  noMemInit: true,      // Performance optimization
  useVersions: true,    // Enable document versioning
  encoding: 'msgpack',  // Data encoding format
})
```

## Testing

LazyDB uses Node.js's built-in test runner with TypeScript support via `tsx`. The test suite includes:

- Unit tests for core functionality
- Integration tests for database operations
- Advanced tests for complex querying scenarios
- Performance tests with large datasets

To run the tests:

```bash
# Run all tests
pnpm test

# Clean build artifacts and coverage
pnpm clean
```

The test suite demonstrates:

- Basic CRUD operations
- Transaction handling
- Query capabilities
- Error scenarios
- Event handling
- Performance with large datasets (10,000+ documents)
- Edge cases and error conditions

## Contributing

Contributions are welcome! LazyDB is built with modern tooling and follows current best practices.

### Development Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/Takin-Profit/LazyDB.git
   cd LazyDB
   ```

2. Install dependencies:

   ```bash
   pnpm install
   ```

3. Build the project:

   ```bash
   pnpm build
   ```

### Project Structure

- Written in TypeScript with full type safety
- Uses ES Modules (ESM)
- Provides both CommonJS and ESM builds
- Includes comprehensive type definitions

### Available Scripts

- `pnpm build` - Build the project using pkgroll
- `pnpm test` - Run the test suite using Node.js test runner with tsx
- `pnpm clean` - Clean build artifacts and coverage reports

### Code Quality

The project uses:

- [Biome](https://biomejs.dev/) for code formatting and linting
- TypeScript for type safety
- Node.js built-in test runner for testing
- pkgroll for building both ESM and CommonJS outputs

### Submitting Changes

1. Fork the repository
2. Create a new branch for your feature or fix
3. Make your changes
4. Run the test suite
5. Submit a pull request

### Package Distribution

The package is distributed on npm as `@takinprofit/lazydb` and includes:

- CommonJS build (`dist/index.cjs`)
- ES Module build (`dist/index.mjs`)
- TypeScript declarations for both formats
- Full source maps

### Issues and Bugs

Please report any issues or bugs on our [GitHub Issues page](https://github.com/Takin-Profit/LazyDB/issues).

## API Reference

For detailed API documentation, please refer to the source code TypeScript definitions or visit our [API Documentation](https://github.com/yourusername/lazydb/docs).

## License

This project is licensed under the BSD License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

LazyDB is built on top of several excellent projects:

- [LMDB](http://www.lmdb.tech/doc/): A lightning-fast, robust key-value store originally developed by Symas Corporation
- [lmdb-js](https://github.com/kriszyp/lmdb-js): An excellent Node.js binding for LMDB that provides the foundation for LazyDB's functionality

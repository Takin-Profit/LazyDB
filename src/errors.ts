// Copyright 2024 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/**
 * Error types for all database operations
 */
export type ErrorType =
	| "UNKNOWN"
	| "VALIDATION"
	| "CONSTRAINT"
	| "TRANSACTION"
	| "NOT_FOUND"
	| "IO"
	| "CORRUPTION"
	| "UPDATE_FAILED"
	| "OPERATION"
/**
 * Base class for all database-related errors
 */
export class DatabaseError extends Error {
	type: ErrorType
	field?: string
	constraint?: string
	original?: unknown
	operation?: string
	key?: string
	txId?: number

	constructor(
		type: ErrorType,
		message: string,
		options: {
			field?: string
			constraint?: string
			original?: unknown
			operation?: string
			key?: string
			txId?: number
		} = {}
	) {
		super(message)
		this.type = type
		this.field = options.field
		this.constraint = options.constraint
		this.original = options.original
		this.operation = options.operation
		this.key = options.key
		this.txId = options.txId
		// Set the prototype explicitly to maintain instanceof behavior
		Object.setPrototypeOf(this, new.target.prototype)
	}
}

/**
 * Specific error classes for different error types
 */
export class ValidationError extends DatabaseError {
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("VALIDATION", message, options)
	}
}

export class ConstraintError extends DatabaseError {
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("CONSTRAINT", message, options)
	}
}

export class TransactionError extends DatabaseError {
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("TRANSACTION", message, options)
	}
}

export class NotFoundError extends DatabaseError {
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("NOT_FOUND", message, options)
	}
}

export class IOError extends DatabaseError {
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("IO", message, options)
	}
}

export class CorruptionError extends DatabaseError {
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("CORRUPTION", message, options)
	}
}

export class UpdateFailedError extends DatabaseError {
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("UPDATE_FAILED", message, options)
	}
}

export class OperationError extends DatabaseError {
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("OPERATION", message, options)
	}
}

export class UnknownError extends DatabaseError {
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("UNKNOWN", message, options)
	}
}

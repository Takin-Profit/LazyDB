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

	/**
	 * @param {ErrorType} type - The type of the database error.
	 * @param {string} message - A descriptive message for the error.
	 * @param {object} [options] - Additional details about the error.
	 * @param {string} [options.field] - The name of the field causing the error, if applicable.
	 * @param {string} [options.constraint] - The constraint that was violated, if applicable.
	 * @param {unknown} [options.original] - The original error or additional data.
	 * @param {string} [options.operation] - The operation that caused the error.
	 * @param {string} [options.key] - The key associated with the error.
	 * @param {number} [options.txId] - The transaction ID related to the error.
	 */
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
 * Represents a conflict error, typically caused by a constraint violation.
 */
export class ConflictError extends DatabaseError {
	/**
	 * @param {string} message - A descriptive message for the conflict error.
	 * @param {object} [options] - Additional details about the error.
	 */
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("CONSTRAINT", message, options)
	}
}

/**
 * Represents a validation error, typically caused by invalid input data.
 */
export class ValidationError extends DatabaseError {
	fields?: string[]

	/**
	 * @param {string} message - A descriptive message for the validation error.
	 * @param {object} [options] - Additional details about the error.
	 * @param {string[]} [options.fields] - The fields that caused the validation error.
	 */
	constructor(
		message: string,
		options?: { fields?: string[] } & ConstructorParameters<
			typeof DatabaseError
		>[2]
	) {
		super("VALIDATION", message, options) // Pass options directly to DatabaseError
		this.fields = options?.fields
	}
}

/**
 * Represents a constraint error, such as exceeding a limit or a unique key conflict.
 */
export class ConstraintError extends DatabaseError {
	/**
	 * @param {string} message - A descriptive message for the constraint error.
	 * @param {object} [options] - Additional details about the error.
	 */
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("CONSTRAINT", message, options)
	}
}

/**
 * Represents an error during a database transaction.
 */
export class TransactionError extends DatabaseError {
	/**
	 * @param {string} message - A descriptive message for the transaction error.
	 * @param {object} [options] - Additional details about the error.
	 */
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("TRANSACTION", message, options)
	}
}

/**
 * Represents an error when a resource is not found.
 */
export class NotFoundError extends DatabaseError {
	/**
	 * @param {string} message - A descriptive message for the not found error.
	 * @param {object} [options] - Additional details about the error.
	 */
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("NOT_FOUND", message, options)
	}
}

/**
 * Represents an input/output (IO) error.
 */
export class IOError extends DatabaseError {
	/**
	 * @param {string} message - A descriptive message for the IO error.
	 * @param {object} [options] - Additional details about the error.
	 */
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("IO", message, options)
	}
}

/**
 * Represents a database corruption error.
 */
export class CorruptionError extends DatabaseError {
	/**
	 * @param {string} message - A descriptive message for the corruption error.
	 * @param {object} [options] - Additional details about the error.
	 */
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("CORRUPTION", message, options)
	}
}

/**
 * Represents an error when an update operation fails.
 */
export class UpdateFailedError extends DatabaseError {
	/**
	 * @param {string} message - A descriptive message for the update failed error.
	 * @param {object} [options] - Additional details about the error.
	 */
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("UPDATE_FAILED", message, options)
	}
}

/**
 * Represents a general error during an operation.
 */
export class OperationError extends DatabaseError {
	/**
	 * @param {string} message - A descriptive message for the operation error.
	 * @param {object} [options] - Additional details about the error.
	 */
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("OPERATION", message, options)
	}
}

/**
 * Represents an unknown error that doesn't fit other categories.
 */
export class UnknownError extends DatabaseError {
	/**
	 * @param {string} message - A descriptive message for the unknown error.
	 * @param {object} [options] - Additional details about the error.
	 */
	constructor(
		message: string,
		options?: Omit<ConstructorParameters<typeof DatabaseError>[2], "type">
	) {
		super("UNKNOWN", message, options)
	}
}

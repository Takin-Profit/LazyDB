// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import { Type } from "@sinclair/typebox"
import { literal, union, object, bool, num, type $, partial } from "./utils.js"

// Define the journal modes schema
const JournalMode = union([
	literal("DELETE"),
	literal("TRUNCATE"),
	literal("PERSIST"),
	literal("MEMORY"),
	literal("WAL"),
	literal("OFF"),
])

// Define the synchronous modes schema
const SynchronousMode = union([
	literal("OFF"),
	literal("NORMAL"),
	literal("FULL"),
	literal("EXTRA"),
])

// Define the temp store schema
const TempStore = union([
	literal("DEFAULT"),
	literal("FILE"),
	literal("MEMORY"),
])

// Define the locking mode schema
const LockingMode = union([literal("NORMAL"), literal("EXCLUSIVE")])

// Extract types from schemas
export type JournalMode = $<typeof JournalMode>
export type SynchronousMode = $<typeof SynchronousMode>
export type TempStore = $<typeof TempStore>
export type LockingMode = $<typeof LockingMode>

// Define the pragma configuration schema
export const PragmaConfig = partial(
	object({
		journalMode: JournalMode,
		synchronous: SynchronousMode,
		cacheSize: num(),
		mmapSize: num(),
		tempStore: TempStore,
		lockingMode: LockingMode,
		busyTimeout: num(),
		foreignKeys: bool(),
		walAutocheckpoint: num(),
		trustedSchema: bool(),
	})
)

// Extract type from schema
export type PragmaConfig = $<typeof PragmaConfig>

// Environment-specific pragma defaults
const BasePragmaConfig = partial(
	object({
		journalMode: JournalMode,
		synchronous: SynchronousMode,
		cacheSize: num(),
		tempStore: TempStore,
		mmapSize: num(),
		lockingMode: LockingMode,
		busyTimeout: num(),
		foreignKeys: bool(),
		walAutocheckpoint: num(),
		trustedSchema: bool(),
	})
)

// Extract type from schema
type BasePragmaConfig = $<typeof BasePragmaConfig>

/**
 * Default pragma configurations for different environments
 */
export const PragmaDefaults: Record<string, BasePragmaConfig> = {
	/**
	 * Development environment defaults - optimized for development workflow
	 */
	development: {
		journalMode: "WAL",
		synchronous: "NORMAL",
		cacheSize: -64000, // 64MB cache
		tempStore: "MEMORY",
		mmapSize: 64000000, // 64MB mmap
		lockingMode: "NORMAL",
		busyTimeout: 5000,
		foreignKeys: true,
		walAutocheckpoint: 1000,
		trustedSchema: true,
	},

	/**
	 * Testing environment defaults - optimized for in-memory testing
	 */
	testing: {
		journalMode: "WAL",
		synchronous: "OFF", // Less durable but faster for testing
		cacheSize: -32000, // 32MB cache is enough for testing
		tempStore: "MEMORY",
		lockingMode: "EXCLUSIVE", // Reduce lock conflicts
		busyTimeout: 5000,
		foreignKeys: true,
		walAutocheckpoint: 1000,
		trustedSchema: true,
	},

	/**
	 * Production environment defaults - optimized for durability and performance
	 */
	production: {
		journalMode: "WAL",
		synchronous: "NORMAL",
		cacheSize: -64000, // 64MB cache
		tempStore: "MEMORY",
		mmapSize: 268435456, // 256MB mmap
		lockingMode: "NORMAL",
		busyTimeout: 10000,
		foreignKeys: true,
		walAutocheckpoint: 1000,
		trustedSchema: false, // Safer default for production
	},
} as const

/**
 * Generates SQLite PRAGMA statements from configuration
 */
export function getPragmaStatements(config: PragmaConfig): string[] {
	const statements: string[] = []

	if (config.journalMode) {
		statements.push(`PRAGMA journal_mode=${config.journalMode};`)
	}

	if (config.synchronous) {
		statements.push(`PRAGMA synchronous=${config.synchronous};`)
	}

	if (config.cacheSize !== undefined) {
		statements.push(`PRAGMA cache_size=${config.cacheSize};`)
	}

	if (config.mmapSize !== undefined) {
		statements.push(`PRAGMA mmap_size=${config.mmapSize};`)
	}

	if (config.tempStore) {
		statements.push(`PRAGMA temp_store=${config.tempStore};`)
	}

	if (config.lockingMode) {
		statements.push(`PRAGMA locking_mode=${config.lockingMode};`)
	}

	if (config.busyTimeout !== undefined) {
		statements.push(`PRAGMA busy_timeout=${config.busyTimeout};`)
	}

	if (config.foreignKeys !== undefined) {
		statements.push(`PRAGMA foreign_keys=${config.foreignKeys ? "ON" : "OFF"};`)
	}

	if (config.walAutocheckpoint !== undefined) {
		statements.push(`PRAGMA wal_autocheckpoint=${config.walAutocheckpoint};`)
	}

	if (config.trustedSchema !== undefined) {
		statements.push(
			`PRAGMA trusted_schema=${config.trustedSchema ? "ON" : "OFF"};`
		)
	}

	return statements
}

// Re-export the schema for validation purposes
export const schemas = {
	JournalMode,
	SynchronousMode,
	TempStore,
	LockingMode,
	PragmaConfig,
	BasePragmaConfig,
} as const

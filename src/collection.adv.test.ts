import { strict as assert } from "node:assert"
import { after, before, beforeEach, describe, test } from "node:test"
import { type Repository, Database, type Entity } from "./index.js"

interface ComplexUser extends Entity {
	email: string
	personalInfo: {
		firstName: string
		lastName: string
		age: number
		address: {
			street: string
			city: string
			country: string
			postalCode: string
		}
	}
	accountDetails: {
		status: "active" | "suspended" | "deleted"
		type: "free" | "premium" | "enterprise"
		subscriptionExpiry: Date | null
		lastLogin: Date
		settings: Record<string, unknown>
	}
	metrics: {
		totalLogins: number
		totalPosts: number
		engagementScore: number
	}
	tags: string[]
	createdAt: Date
	updatedAt: Date
}

const TEST_DB_PATH = "advanced-test-db"
const REPOSITORY_NAME = "complex-users"
const DATASET_SIZE = 10000
const BATCH_SIZE = 1000

let db: Database
let users: Repository<ComplexUser>
let testData: Omit<ComplexUser, "_id">[]

function generateTestUser(index: number): Omit<ComplexUser, "_id"> {
	return {
		email: `user${index}@example.com`,
		personalInfo: {
			firstName: `FirstName${index}`,
			lastName: `LastName${index}`,
			age: Math.floor(Math.random() * 62) + 18,
			address: {
				street: `${Math.floor(Math.random() * 9999)} Main St`,
				city: ["New York", "London", "Tokyo"][Math.floor(Math.random() * 3)],
				country: ["USA", "UK", "Japan"][Math.floor(Math.random() * 3)],
				postalCode: String(Math.floor(Math.random() * 90000) + 10000),
			},
		},
		accountDetails: {
			status: ["active", "suspended", "deleted"][
				Math.floor(Math.random() * 3)
			] as "active" | "suspended" | "deleted",
			type: ["free", "premium", "enterprise"][Math.floor(Math.random() * 3)] as
				| "free"
				| "premium"
				| "enterprise",
			subscriptionExpiry: Math.random() > 0.5 ? new Date(2025, 0, 1) : null,
			lastLogin: new Date(),
			settings: {
				isEmailVerified: Math.random() > 0.1,
				language: ["en", "es", "fr"][Math.floor(Math.random() * 3)],
			},
		},
		metrics: {
			totalLogins: Math.floor(Math.random() * 1000),
			totalPosts: Math.floor(Math.random() * 500),
			engagementScore: Math.floor(Math.random() * 100),
		},
		tags: ["active", "premium"].slice(0, Math.floor(Math.random() * 2) + 1),
		createdAt: new Date(2020, 0, 1),
		updatedAt: new Date(),
	}
}

before(async () => {
	db = new Database(TEST_DB_PATH, {
		compression: true,
		pageSize: 8192,
		overlappingSync: true,
		maxRepositories: 10,
	})
	users = db.repository<ComplexUser>(REPOSITORY_NAME)
	testData = Array(DATASET_SIZE)
		.fill(null)
		.map((_, i) => generateTestUser(i))
})

beforeEach(async () => {
	await db.clearRepository(REPOSITORY_NAME)
})

after(async () => {
	await db.dropRepository(REPOSITORY_NAME)
	await db.close()
})

describe("Advanced Repository Operations", { timeout: 30000 }, async () => {
	await test("Batch inserts with data integrity check", async () => {
		const batches: Promise<ComplexUser[]>[] = []
		for (let i = 0; i < DATASET_SIZE; i += BATCH_SIZE) {
			const batch = testData.slice(i, i + BATCH_SIZE)
			batches.push(users.insertMany(batch))
		}

		const results: ComplexUser[][] = await Promise.all(batches)
		const totalInserted = results.reduce((sum, batch) => sum + batch.length, 0)
		assert.equal(totalInserted, DATASET_SIZE)

		const allUsers = users.find().asArray
		assert.equal(allUsers.length, DATASET_SIZE)

		// Verify no duplicate IDs
		const ids = new Set(allUsers.map((u) => u._id))
		assert.equal(ids.size, DATASET_SIZE)
	})

	await test("Complex query filtering", async () => {
		await users.insertMany(testData.slice(0, 1000))

		const results = users.find({
			where: (user) => {
				const isPremiumUser = user.accountDetails.type === "premium"
				const isHighEngagement = user.metrics.engagementScore > 80
				const isActiveUser = user.accountDetails.status === "active"

				return isPremiumUser && isHighEngagement && isActiveUser
			},
		}).asArray

		for (const user of results) {
			assert.equal(user.accountDetails.type, "premium")
			assert(user.metrics.engagementScore > 80)
			assert.equal(user.accountDetails.status, "active")
		}
	})

	await test("Range queries with pagination", async () => {
		await users.insertMany(testData.slice(0, 1000))
		const pageSize = 50
		const pages: ComplexUser[][] = []

		for (let i = 0; i < 5; i++) {
			const page = users.find({
				where: (user) => user.metrics.engagementScore > 50,
				offset: i * pageSize,
				limit: pageSize,
			}).asArray

			pages.push(page)
			assert(page.length <= pageSize)

			if (i > 0) {
				const currentIds = new Set(page.map((u) => u._id))
				const prevIds = new Set(pages[i - 1].map((u) => u._id))
				const overlap = [...currentIds].filter((id) => prevIds.has(id))
				assert.equal(overlap.length, 0)
			}
		}
	})

	await test("Large entity handling", async () => {
		const largeUser = {
			...testData[0],
			accountDetails: {
				...testData[0].accountDetails,
				settings: Object.fromEntries(
					Array(1000)
						.fill(null)
						.map((_, i) => [`setting${i}`, `value${i}`])
				),
			},
		}

		const inserted = await users.insert(largeUser)
		const retrieved = users.get(inserted._id)

		assert(retrieved)
		assert.equal(Object.keys(retrieved.accountDetails.settings).length, 1000)
	})

	await test("Multi-condition updates", async () => {
		const testUsers = Array(100)
			.fill(null)
			.map((_, i) => ({
				...generateTestUser(i),
				accountDetails: {
					status: "active" as const,
					type: i < 10 ? ("free" as const) : ("premium" as const),
					subscriptionExpiry: null,
					lastLogin: new Date(),
					settings: {},
				},
				metrics: {
					totalLogins: 100,
					totalPosts: 200,
					engagementScore: i < 10 ? 80 : 30,
				},
			}))

		await users.insertMany(testUsers)

		const updatedCount = await users.updateMany(
			{
				where: (user) =>
					user.accountDetails.type === "free" &&
					user.metrics.engagementScore > 70,
			},
			{
				accountDetails: {
					status: "active" as const,
					type: "premium" as const,
					subscriptionExpiry: new Date(2025, 0, 1),
					lastLogin: new Date(),
					settings: {},
				},
			}
		)

		assert.equal(updatedCount, 10)

		const verifyUpdates = users.find({
			where: (user) =>
				user.accountDetails.type === "premium" &&
				user.metrics.engagementScore > 70,
		}).asArray

		assert.equal(verifyUpdates.length, 10)
	})

	await test("Special character handling", async () => {
		const specialUser = {
			...testData[0],
			personalInfo: {
				...testData[0].personalInfo,
				firstName: "Test 象形文字",
				lastName: "User 🌟 テスト",
			},
		}

		const inserted = await users.insert(specialUser)
		const retrieved = users.get(inserted._id)

		assert(retrieved)
		assert.equal(
			retrieved.personalInfo.firstName,
			specialUser.personalInfo.firstName
		)
		assert.equal(
			retrieved.personalInfo.lastName,
			specialUser.personalInfo.lastName
		)
	})

	await test("Performance: Complex filtering", async () => {
		await users.insertMany(testData)

		const start = Date.now()
		const results = users.find({
			where: (user) => {
				const ageGroup = Math.floor(user.personalInfo.age / 10) * 10
				const isActivePayingUser =
					user.accountDetails.status === "active" &&
					user.accountDetails.type !== "free"
				const hasHighEngagement =
					user.metrics.engagementScore > 70 && user.metrics.totalPosts > 100

				return ageGroup >= 30 && isActivePayingUser && hasHighEngagement
			},
		}).asArray

		const duration = Date.now() - start
		assert(duration < 5000)

		for (const user of results) {
			assert(Math.floor(user.personalInfo.age / 10) * 10 >= 30)
			assert(user.accountDetails.status === "active")
			assert(user.accountDetails.type !== "free")
			assert(user.metrics.engagementScore > 70)
			assert(user.metrics.totalPosts > 100)
		}
	})
})

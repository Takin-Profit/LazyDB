// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/**
 * Ensure all keys in a tuple are distinct at compile time.
 */

// Base type: Ensure K1 != K2, ..., Kn
type DistinctKeys2<K1 extends string, K2 extends string> = K2 extends K1
	? never
	: [K1, K2]

type DistinctKeys3<
	K1 extends string,
	K2 extends string,
	K3 extends string,
> = K2 extends K1
	? never
	: K3 extends K1
		? never
		: K3 extends K2
			? never
			: [K1, K2, K3]

type DistinctKeys4<
	K1 extends string,
	K2 extends string,
	K3 extends string,
	K4 extends string,
> = K2 extends K1
	? never
	: K3 extends K1 | K2
		? never
		: K4 extends K1 | K2 | K3
			? never
			: [K1, K2, K3, K4]

type DistinctKeys5<
	K1 extends string,
	K2 extends string,
	K3 extends string,
	K4 extends string,
	K5 extends string,
> = K2 extends K1
	? never
	: K3 extends K1 | K2
		? never
		: K4 extends K1 | K2 | K3
			? never
			: K5 extends K1 | K2 | K3 | K4
				? never
				: [K1, K2, K3, K4, K5]

type DistinctKeys6<
	K1 extends string,
	K2 extends string,
	K3 extends string,
	K4 extends string,
	K5 extends string,
	K6 extends string,
> = K2 extends K1
	? never
	: K3 extends K1 | K2
		? never
		: K4 extends K1 | K2 | K3
			? never
			: K5 extends K1 | K2 | K3 | K4
				? never
				: K6 extends K1 | K2 | K3 | K4 | K5
					? never
					: [K1, K2, K3, K4, K5, K6]

// Extend this pattern up to 10 keys...

type DistinctKeys7<
	K1 extends string,
	K2 extends string,
	K3 extends string,
	K4 extends string,
	K5 extends string,
	K6 extends string,
	K7 extends string,
> = K2 extends K1
	? never
	: K3 extends K1 | K2
		? never
		: K4 extends K1 | K2 | K3
			? never
			: K5 extends K1 | K2 | K3 | K4
				? never
				: K6 extends K1 | K2 | K3 | K4 | K5
					? never
					: K7 extends K1 | K2 | K3 | K4 | K5 | K6
						? never
						: [K1, K2, K3, K4, K5, K6, K7]

type DistinctKeys8<
	K1 extends string,
	K2 extends string,
	K3 extends string,
	K4 extends string,
	K5 extends string,
	K6 extends string,
	K7 extends string,
	K8 extends string,
> = K2 extends K1
	? never
	: K3 extends K1 | K2
		? never
		: K4 extends K1 | K2 | K3
			? never
			: K5 extends K1 | K2 | K3 | K4
				? never
				: K6 extends K1 | K2 | K3 | K4 | K5
					? never
					: K7 extends K1 | K2 | K3 | K4 | K5 | K6
						? never
						: K8 extends K1 | K2 | K3 | K4 | K5 | K6 | K7
							? never
							: [K1, K2, K3, K4, K5, K6, K7, K8]

// Similarly, define DistinctKeys9 and DistinctKeys10...

type DistinctKeys9<
	K1 extends string,
	K2 extends string,
	K3 extends string,
	K4 extends string,
	K5 extends string,
	K6 extends string,
	K7 extends string,
	K8 extends string,
	K9 extends string,
> = K2 extends K1
	? never
	: K3 extends K1 | K2
		? never
		: K4 extends K1 | K2 | K3
			? never
			: K5 extends K1 | K2 | K3 | K4
				? never
				: K6 extends K1 | K2 | K3 | K4 | K5
					? never
					: K7 extends K1 | K2 | K3 | K4 | K5 | K6
						? never
						: K8 extends K1 | K2 | K3 | K4 | K5 | K6 | K7
							? never
							: K9 extends K1 | K2 | K3 | K4 | K5 | K6 | K7 | K8
								? never
								: [K1, K2, K3, K4, K5, K6, K7, K8, K9]

type DistinctKeys10<
	K1 extends string,
	K2 extends string,
	K3 extends string,
	K4 extends string,
	K5 extends string,
	K6 extends string,
	K7 extends string,
	K8 extends string,
	K9 extends string,
	K10 extends string,
> = K2 extends K1
	? never
	: K3 extends K1 | K2
		? never
		: K4 extends K1 | K2 | K3
			? never
			: K5 extends K1 | K2 | K3 | K4
				? never
				: K6 extends K1 | K2 | K3 | K4 | K5
					? never
					: K7 extends K1 | K2 | K3 | K4 | K5 | K6
						? never
						: K8 extends K1 | K2 | K3 | K4 | K5 | K6 | K7
							? never
							: K9 extends K1 | K2 | K3 | K4 | K5 | K6 | K7 | K8
								? never
								: K10 extends K1 | K2 | K3 | K4 | K5 | K6 | K7 | K8 | K9
									? never
									: [K1, K2, K3, K4, K5, K6, K7, K8, K9, K10]

/**
 * Combine all valid groupBy tuples
 */
export type GroupByTuples<K extends string> =
	// length = 1
	| [K]
	// length = 2
	| {
			[A in K]: {
				[B in K]: DistinctKeys2<A, B>
			}[K]
	  }[K]
	// length = 3
	| {
			[A in K]: {
				[B in K]: {
					[C in K]: DistinctKeys3<A, B, C>
				}[K]
			}[K]
	  }[K]
	// length = 4
	| {
			[A in K]: {
				[B in K]: {
					[C in K]: {
						[D in K]: DistinctKeys4<A, B, C, D>
					}[K]
				}[K]
			}[K]
	  }[K]
	// length = 5
	| {
			[A in K]: {
				[B in K]: {
					[C in K]: {
						[D in K]: {
							[E in K]: DistinctKeys5<A, B, C, D, E>
						}[K]
					}[K]
				}[K]
			}[K]
	  }[K]
	// length = 6
	| {
			[A in K]: {
				[B in K]: {
					[C in K]: {
						[D in K]: {
							[E in K]: {
								[F in K]: DistinctKeys6<A, B, C, D, E, F>
							}[K]
						}[K]
					}[K]
				}[K]
			}[K]
	  }[K]
	// length = 7
	| {
			[A in K]: {
				[B in K]: {
					[C in K]: {
						[D in K]: {
							[E in K]: {
								[F in K]: {
									[G in K]: DistinctKeys7<A, B, C, D, E, F, G>
								}[K]
							}[K]
						}[K]
					}[K]
				}[K]
			}[K]
	  }[K]
	// length = 8
	| {
			[A in K]: {
				[B in K]: {
					[C in K]: {
						[D in K]: {
							[E in K]: {
								[F in K]: {
									[G in K]: {
										[H in K]: DistinctKeys8<A, B, C, D, E, F, G, H>
									}[K]
								}[K]
							}[K]
						}[K]
					}[K]
				}[K]
			}[K]
	  }[K]
	// length = 9
	| {
			[A in K]: {
				[B in K]: {
					[C in K]: {
						[D in K]: {
							[E in K]: {
								[F in K]: {
									[G in K]: {
										[H in K]: {
											[I in K]: DistinctKeys9<A, B, C, D, E, F, G, H, I>
										}[K]
									}[K]
								}[K]
							}[K]
						}[K]
					}[K]
				}[K]
			}[K]
	  }[K]
	// length = 10
	| {
			[A in K]: {
				[B in K]: {
					[C in K]: {
						[D in K]: {
							[E in K]: {
								[F in K]: {
									[G in K]: {
										[H in K]: {
											[I in K]: {
												[J in K]: DistinctKeys10<A, B, C, D, E, F, G, H, I, J>
											}[K]
										}[K]
									}[K]
								}[K]
							}[K]
						}[K]
					}[K]
				}[K]
			}[K]
	  }[K]

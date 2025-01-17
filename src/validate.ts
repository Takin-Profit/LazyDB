// Copyright 2025 Takin Profit. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

export type ValidationError = {
	_lazy_validation_error: true
	message: string
	path?: string
}

export const isValidationErr = (value: unknown): value is ValidationError => {
	return (
		typeof value === "object" &&
		value !== null &&
		"_lazy_validation_error" in value
	)
}

export const isValidationErrs = (value: unknown): value is ValidationError[] =>
	Array.isArray(value) && value.every(isValidationErr)

export const validationErr = ({
	msg: message,
	path,
}: { msg: string; path?: string }): ValidationError => ({
	_lazy_validation_error: true,
	message,
	path,
})

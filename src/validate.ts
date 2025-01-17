export type ValidationError = {
	_lazy_validation_error: true // Note the underscore
	message: string
	path?: string
}

export const isValidationErr = (value: unknown): value is ValidationError => {
	return (
		typeof value === "object" &&
		value !== null &&
		"_lazy_validation_error" in value // Add the underscore here
	)
}

export const isValidationErrs = (value: unknown): value is ValidationError[] =>
	Array.isArray(value) && value.length > 0 && value.every(isValidationErr)

export const validationErr = ({
	msg: message,
	path,
}: { msg: string; path?: string }): ValidationError => ({
	_lazy_validation_error: true, // Add the underscore here
	message,
	path,
})

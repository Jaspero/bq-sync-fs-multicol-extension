export function safeFloat(value: number) {
	if (!value && value !== 0) {
		return null;
	}

	if (Number.isInteger(value)) {
		return value;
	}

	return parseFloat(value.toFixed(2));
}
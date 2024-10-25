package accumulator

func CountingTrigger[T any](n int) ValueTrigger[T] {
	return countingTrigger[T](n)
}

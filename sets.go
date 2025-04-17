package rink

// Difference returns a - b, i.e. the keys in `a` that are not in `b`
func Difference[T comparable, V1, V2 any](a map[T]V1, b map[T]V2) []T {
	var ret []T
	for k := range a {
		if _, ok := b[k]; ok {
			continue
		}
		ret = append(ret, k)
	}
	return ret
}

// Intersect returns the keys that are in `a` and `b`
func Intersect[T comparable, V1, V2 any](a map[T]V1, b map[T]V2) []T {
	var ret []T
	for k := range a {
		if _, ok := b[k]; !ok {
			continue
		}
		ret = append(ret, k)
	}
	return ret
}

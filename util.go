package mvcc

func _assert(condition bool, failedPrompt string) {
	if len(failedPrompt) == 0 {
		failedPrompt = "condition is unsatisfied"
	}
	if !condition {
		panic(failedPrompt)
	}
}

func max(a, b txid) txid {
	if a > b {
		return a
	}
	return b
}

func clone(src map[txid]struct{}, dst *map[txid]struct{}) {
	for k := range src {
		(*dst)[k] = struct{}{}
	}
}

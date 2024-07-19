package bookkeeper

func BytesEqual(bs1, bs2 []byte) bool {
	if len(bs1) != len(bs2) {
		return false
	}

	for i, c := range bs1 {
		if c != bs2[i] {
			return false
		}
	}
	return true
}

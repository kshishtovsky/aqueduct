package broker

import "bytes"

// MatchWildcard checks if topic matches pattern (supporting '+' and '#') with 0 heap allocations.
// MQTT wildcard rules:
//   '+' matches exactly one topic level (segment between '/').
//   '#' matches zero or more topic levels up to the end of topic.
func MatchWildcard(pattern, topic []byte) bool {
	if bytes.Equal(pattern, topic) {
		return true
	}

	pLen, tLen := len(pattern), len(topic)
	pIdx, tIdx := 0, 0

	for pIdx < pLen {
		if pattern[pIdx] == '#' {
			// '#' matches 0 or more topic levels
			return true
		}

		if pattern[pIdx] == '+' {
			// '+' matches one topic segment up to next '/'
			if tIdx >= tLen {
				return false
			}
			for tIdx < tLen && topic[tIdx] != '/' {
				tIdx++
			}
			pIdx++
			continue
		}

		if tIdx >= tLen {
			// Check if pattern ends with "/#" while topic ended right before '/'
			if pattern[pIdx] == '/' && pIdx+1 < pLen && pattern[pIdx+1] == '#' {
				return true
			}
			return false
		}

		if pattern[pIdx] != topic[tIdx] {
			return false
		}
		pIdx++
		tIdx++
	}

	return pIdx == pLen && tIdx == tLen
}

// IsWildcardTopic returns true if topic string contains '+' or '#'.
func IsWildcardTopic(topic string) bool {
	return bytes.IndexByte([]byte(topic), '+') >= 0 || bytes.IndexByte([]byte(topic), '#') >= 0
}

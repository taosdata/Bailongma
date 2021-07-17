package util

import "strings"

func EscapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

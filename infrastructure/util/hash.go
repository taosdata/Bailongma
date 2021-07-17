package util

import (
	"crypto/md5"
	"fmt"
)

func ToHashString(source string) string {
	return fmt.Sprintf("md5_%x", md5.Sum([]byte(source)))
}

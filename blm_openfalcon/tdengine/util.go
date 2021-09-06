package tdengine

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func float2String(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func slice2String(value []string) string {
	for i, s := range value {
		if s == "" {
			value[i] = "null"
		}
	}
	return strings.Join(value, ",")
}

func second2String(t int64) string {
	return fmt.Sprintf("'%s'", time.Unix(t, 0).Format(time.RFC3339Nano))
}

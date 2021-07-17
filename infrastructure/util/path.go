package util

import "os"

func PathExist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

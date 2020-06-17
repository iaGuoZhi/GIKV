package utilservice

import (
	"log"
	"math/rand"
	"strings"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

const DebugMode = false

func RandStringBytesMaskImpr(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.New(rand.NewSource(time.Now().UnixNano())).Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.New(rand.NewSource(time.Now().UnixNano())).Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

// CompareZkChildren ... get znode's children change after childrenW trigger
func CompareZkChildren(childrenNow []string, childrenBefore []string) ([]string, []string) {
	var addChildren []string
	var deleteChildren []string

	for _, childNow := range childrenNow {
		exist := false
		for _, childBefore := range childrenBefore {
			if strings.Compare(childNow, childBefore) == 0 {
				exist = true
			}
		}
		if exist == false {
			addChildren = append(addChildren, childNow)
		}
	}

	for _, childBefore := range childrenBefore {
		exist := false
		for _, childNow := range childrenNow {
			if strings.Compare(childNow, childBefore) == 0 {
				exist = true
			}
		}
		if exist == false {
			deleteChildren = append(deleteChildren, childBefore)
		}
	}

	return addChildren, deleteChildren
}

// MyPrintln ... print string only debugMode is true
func MyPrintln(s string) {
	if DebugMode {
		log.Println(s)
	}
}

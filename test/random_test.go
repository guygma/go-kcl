package utils

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestRandom(t *testing.T) {
	for i := 0; i < 10; i++ {
		s1 := RandStringBytesMaskImpr(10)
		s2 := RandStringBytesMaskImpr(10)
		if s1 == s2 {
			t.Fatalf("failed in generating random string. s1: %s, s2: %s", s1, s2)
		}
	}
}

func TestRandomNum(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())

	for i := 0; i < 10; i++ {
		s1 := rand.Int63()
		s2 := rand.Int63()
		if s1 == s2 {
			t.Fatalf("failed in generating random string. s1: %d, s2: %d", s1, s2)
		}
		fmt.Println(s1)
		fmt.Println(s2)
	}
}

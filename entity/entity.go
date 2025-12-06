package entity

import (
	"math/rand"
	"time"
)

type User struct {
	Fields [99]string
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func GenerateUser() User {
	var u User
	for i := 0; i < 99; i++ {
		u.Fields[i] = randString(16)
	}
	return u
}

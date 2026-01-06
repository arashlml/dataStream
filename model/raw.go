package model

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Collection struct {
	RawCollection RawCollection `json:"raw_collection"`
	Cursor        Cursor        `json:"Cursor"`
}
type Cursor map[string]interface{}
type RawCollection []map[string]interface{}

func (c RawCollection) LastItemID() string {
	if len(c) == 0 {
		return ""
	}
	if lastItemID, ok := c[len(c)-1]["_id"]; ok {
		switch t := lastItemID.(type) {
		case string:
			return t
		case primitive.ObjectID:
			return t.Hex()
		default:
			return fmt.Sprint("no valid id found")
		}
	}
	return "no id found"
}
func (c RawCollection) Len() int {
	return len(c)
}
func (c RawCollection) Raw() []map[string]interface{} {
	return c
}

package entity

type RawCollection []map[string]interface{}

func (c RawCollection) LastItemID() string {
	if len(c) == 0 {
		return ""
	}
	if lastItemID, ok := c[len(c)-1]["_id"]; !ok {
		return ""
	} else {
		return lastItemID.(string)
	}
}
func (c RawCollection) Len() int {
	return len(c)
}
func (c RawCollection) Raw() []map[string]interface{} {
	return c
}

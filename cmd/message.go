package cmd

import (
	"encoding/json"

	"github.com/bxcodec/faker/v3"
)

type Message struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func NewRandomMessage() Message {
	age, _ := faker.RandomInt(18, 90, 1)
	return Message{
		ID:   faker.UUIDHyphenated(),
		Name: faker.Name(),
		Age:  age[0],
	}
}

func (m Message) SerializeJSON() string {
	raw, _ := json.Marshal(m)
	return string(raw)
}

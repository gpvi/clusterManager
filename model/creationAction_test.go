package model

import (
	"log"
	"testing"
)

func TestCreation(t *testing.T) {
	err := CreateAction(3, 2)
	if err != nil {
		log.Printf("Error: %v", err)
	}
}

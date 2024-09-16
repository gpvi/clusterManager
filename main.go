package main

import (
	"log"
	"redisStudy/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

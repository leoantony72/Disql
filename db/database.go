package db

import (
	"fmt"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func StartDb(file string) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(file), &gorm.Config{})
	if err != nil {
		fmt.Println("Error: %s", err.Error())
		return nil, err
	}
	return db, nil
}

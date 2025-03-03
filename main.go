package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/leoantony72/disql/db"
	"gorm.io/gorm"
)

func main() {
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		fmt.Println("could not listen to port 8000")
		lis.Close()
		os.Exit(1)
	}
	fmt.Println("tcp server listening on port :8000")
	db, _ := db.StartDb()
	for {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Printf("could not accept conn from %s", conn.RemoteAddr())
			conn.Close()
			continue
		}

		conn.Write([]byte("connected to tcp server :8000\n"))
		ReceiveMessages(conn, db)
	}
}

func ReceiveMessages(c net.Conn, db *gorm.DB) {
	buffer := make([]byte, 100)
	defer c.Close()
	for {
		n, err := c.Read(buffer)
		if err != nil {
			c.Write([]byte("Couldn't read the message!\n"))
			c.Close()
			continue
		}
		cmd := string(buffer[0:n])
		ExecuteSQL(c, db, cmd)

	}
}

func ExecuteSQL(c net.Conn, db *gorm.DB, cmd string) {
	if isQueryReturningResults(cmd) {
		rows, err := db.Raw(cmd).Rows()
		if err != nil {
			c.Write([]byte("SQL Error: " + err.Error() + "\n"))
			return
		}
		defer rows.Close()

		var results []string
		cols, _ := rows.Columns()

		// Iterate over rows
		for rows.Next() {
			values := make([]interface{}, len(cols))
			valuePtrs := make([]interface{}, len(cols))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				c.Write([]byte("Error reading results\n"))
				continue
			}

			rowStr := ""
			for i, col := range cols {
				rowStr += fmt.Sprintf("%s: %v | ", col, values[i])
			}
			results = append(results, rowStr)
		}

		// Send results to the client
		if len(results) > 0 {
			c.Write([]byte(strings.Join(results, "\n") + "\n"))
		} else {
			c.Write([]byte("No results found.\n"))
		}
	} else {
		// Execute command without returning results
		if err := db.Exec(cmd).Error; err != nil {
			c.Write([]byte("SQL Error: " + err.Error() + "\n"))
		} else {
			c.Write([]byte("Query executed successfully.\n"))
		}
	}
}

func isQueryReturningResults(query string) bool {
	query = strings.TrimSpace(strings.ToLower(query))
	return strings.HasPrefix(query, "select") || strings.HasPrefix(query, "show") ||
		strings.HasPrefix(query, "describe") || strings.HasPrefix(query, "explain")
}

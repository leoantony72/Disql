package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		fmt.Println("could not listen to port 8000")
		lis.Close()
		os.Exit(1)
	}
	fmt.Println("tcp server listening on port :8000")

	for {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Printf("could not accept conn from %s", conn.RemoteAddr())
			conn.Close()
			continue
		}

		conn.Write([]byte("connected to tcp server :8000\n"))
		ReceiveMessages(conn)
	}
}

func ReceiveMessages(c net.Conn) {
	buffer := make([]byte, 100)
	for {
		n, err := c.Read(buffer)
		if err != nil {
			c.Write([]byte("Couldn't read the message!\n"))
			c.Close()
			continue
		}
		c.Write(buffer[0:n])
	}
}

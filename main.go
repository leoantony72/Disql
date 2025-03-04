package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/leoantony72/disql/db"
	pb "github.com/leoantony72/disql/disql"
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

type server struct {
	pb.UnimplementedParticipantServer
	mu         sync.Mutex
	connection []string
	data       map[string]string
	db         *gorm.DB
}

func (s *server) NewConnection(ctx context.Context, req *pb.NewConnectionRequest) (*pb.NewConnectionResponse, error) {
	s.connection = append(s.connection, req.Url)
	return &pb.NewConnectionResponse{Success: true}, nil
}
func (s *server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	if _, ok := s.data[req.CmdId]; !ok {
		s.data[req.CmdId] = req.Cmd
		return &pb.PrepareResponse{Success: true}, nil
	}
	return &pb.PrepareResponse{Success: false}, nil
}

func (s *server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	if val, ok := s.data[req.CmdId]; ok {
		succes := s.ExecuteSQLGrpc(val)
		if !succes {
			return &pb.CommitResponse{Success: false}, nil
		}
		delete(s.data, req.CmdId)
		return &pb.CommitResponse{Success: true}, nil
	}
	return &pb.CommitResponse{Success: false}, nil
}

func (s *server) RollBack(ctx context.Context, req *pb.RollBackRequest) (*pb.RollBackResponse, error) {
	delete(s.data, req.CmdId)
	return &pb.RollBackResponse{Success: true}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		fmt.Println("could not listen to port 8000")
		lis.Close()
		os.Exit(1)
	}
	fmt.Println("tcp server listening on port :8000")
	db, _ := db.StartDb()
	grpcServer := grpc.NewServer()
	server := &server{}
	pb.RegisterParticipantServer(grpcServer, server)

	for {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Printf("could not accept conn from %s", conn.RemoteAddr())
			conn.Close()
			continue
		}

		conn.Write([]byte("connected to tcp server :8000\n"))
		ReceiveMessages(conn, db, server)
	}
}

func ReceiveMessages(c net.Conn, db *gorm.DB, s *server) {
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

func Coordinate(s *server) {
	for _,v := range s.connection{
		
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

func (s *server) ExecuteSQLGrpc(cmd string) bool {
	if isQueryReturningResults(cmd) {
		rows, err := s.db.Raw(cmd).Rows()
		if err != nil {
			return false
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
				continue
			}

			rowStr := ""
			for i, col := range cols {
				rowStr += fmt.Sprintf("%s: %v | ", col, values[i])
			}
			results = append(results, rowStr)
		}
	} else {
		// Execute command without returning results
		if err := s.db.Exec(cmd).Error; err != nil {
			return false
		} else {
			return true
		}
	}
	return false
}

func isQueryReturningResults(query string) bool {
	query = strings.TrimSpace(strings.ToLower(query))
	return strings.HasPrefix(query, "select") || strings.HasPrefix(query, "show") ||
		strings.HasPrefix(query, "describe") || strings.HasPrefix(query, "explain")
}

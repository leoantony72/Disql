package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/leoantony72/disql/db"
	pb "github.com/leoantony72/disql/disql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

var port string
var rpcPort string
var dbFile string

func main() {
	server := &server{}
	server.connection = os.Args[1:]
	server.data = make(map[string]string)

	var connStr string
	flag.StringVar(&connStr, "connections", "", "Comma-separated list of connection URLs")
	// Define a flag for the port
	flag.StringVar(&port, "port", "50051", "Server port (default: 50051)")
	flag.StringVar(&rpcPort, "rpc", "70", "rpcPort port (default: 70)")
	flag.StringVar(&dbFile, "file", "gorm.db", "default file name (default: gorm.db)")

	flag.Parse()
	if connStr != "" {
		server.connection = strings.Split(connStr, ",")
	}
	port = ":" + port

	lis, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("could not listen to port %s\n", port)
		lis.Close()
		os.Exit(1)
	}
	fmt.Printf("tcp server listening on port %s\n", port)
	db, _ := db.StartDb(dbFile)
	server.db = db

	go func() {
		grpcPort := ":" + rpcPort // Ensure correct format
		lis, err := net.Listen("tcp", grpcPort)
		if err != nil {
			fmt.Printf("could not listen to port %s\n", rpcPort)
			lis.Close()
			os.Exit(1)
		}
		grpcServer := grpc.NewServer()
		pb.RegisterParticipantServer(grpcServer, server)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve gRPC: %v\n", err)
		}
	}()

	for {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Printf("could not accept conn from %s\n", conn.RemoteAddr())
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

		ok := Coordinate(s, cmd)
		if ok {
			ExecuteSQL(c, db, cmd)
		} else {
			c.Write([]byte("Replication failed!!\n"))
		}
	}
}

func Coordinate(s *server, cmd string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	success := true
	for _, url := range s.connection {
		fmt.Printf("Coordinating with client: %s\n", url)

		// Connect to the client
		conn, err := grpc.Dial(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect to client %s: %v", url, err)
			success = false
			break
		}
		defer conn.Close()
		id := uuid.New().String()
		client := pb.NewParticipantClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		// Prepare Phase
		prepRes, err := client.Prepare(ctx, &pb.PrepareRequest{CmdId: id, Cmd: cmd})
		if err != nil || !prepRes.Success {
			log.Printf("Prepare failed for client %s: %v :%s", url, err,cmd)
			success = false
			break
		}
		fmt.Printf("Prepare successful for client %s : %s\n", url,cmd)

		// Commit Phase
		commitRes, err := client.Commit(ctx, &pb.CommitRequest{CmdId: id})
		if err != nil || !commitRes.Success {
			log.Printf("Commit failed for client %s: %v", url, err)
			success = false
			break
		}
		success = true
		fmt.Printf("Commit successful for client %s\n", url)
	}
	return success
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
		return true
	} else {
		// Execute command without returning results
		if err := s.db.Exec(cmd).Error; err != nil {
			return false
		} else {
			return true
		}
	}
	
}

func isQueryReturningResults(query string) bool {
	query = strings.TrimSpace(strings.ToLower(query))
	return strings.HasPrefix(query, "select") || strings.HasPrefix(query, "show") ||
		strings.HasPrefix(query, "describe") || strings.HasPrefix(query, "explain")
}

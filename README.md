# **DisQL - Distributed Sqlite Server with Go**

DisQL is a distributed sqlite server written in golang, with replication done with 2-phase commit. It's suitable for your next hobby projects requiring limited database usage.

### Installation

1. Clone the repository: **`git clone https://github.com/leoantony72/Disql.git`**
2. Navigate to the project directory: **`cd Disql`**
3. Install dependencies: **`go mod tidy`**
5. Start the project:
```go
go run main.go -port=9050 -connections=localhost:8080 -file=db.db -rpc=9090

go run main.go -port=8000 -connections=localhost:9090 -file=gorm.db -rpc=8080
```

### Usage

***Connect to the server 8000/9000***
```go
    telnet localhost 8000
```

***Execute Sql commands***
```sql
   create table student(id int, name varchar(20), age int);
```


## **Contributing**

If you'd like to contribute to Project Title, here are some guidelines:

1. Fork the repository.
2. Create a new branch for your changes.
3. Make your changes.
4. Write tests to cover your changes.
5. Run the tests to ensure they pass.
6. Commit your changes.
7. Push your changes to your forked repository.
8. Submit a pull request.
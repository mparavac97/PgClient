package main

import (
	"flag"
	"fmt"

	"github.com/mparavac97/PgClient/pkg/client"
)

const (
	host       = "192.168.0.25"
	port       = "5433"
	username   = "mislavclient"
	database   = "postgres"
	password   = "postgres"
	connString = "Host=192.168.0.25;Port=5433;Database=postgres;Username=mislavclient;Password=postgres;"
)

func main() {
	query := parseArguments()
	// query := "explain analyze SELECT * FROM \"TestTable\";"
	query = "insert into \"TestTable\" (\"Id\", \"Name\", \"Age\") values (gen_random_uuid(), 'Jane Doe', 99);"
	fmt.Println("Query to execute:", query)

	pgConn := client.NewPgConnection(connString)
	err := pgConn.Connect()
	if err != nil {
		panic(err)
	}
	defer pgConn.Close()

	cmd := client.NewPgCommand("UPDATE \"TestTable\" SET \"Age\" = 55 WHERE \"Name\" = $1;", pgConn)
	cmd.SetParameter("$1", "Sarah Wilson")
	result, err := cmd.Execute()
	if err != nil {
		panic(err)
	}
	for _, value := range result.Rows {
		fmt.Println("Value:", value)
	}
	fmt.Println(pgConn.TransactionStatus)
}

func parseArguments() string {
	query := flag.String("q", "", "SQL query to execute")
	flag.Parse()

	return *query
}

// func handleAuthentication(conn net.Conn, user, pass string) error {
// 	for {
// 		msgType := readByte(conn)
// 		//length := readInt32(conn)
// 		fmt.Print("Message type:")
// 		fmt.Println(msgType)
// 		switch msgType {
// 		case 'R': //authentication
// 			authType := readInt32(conn)
// 			switch authType {
// 			case 0: //OK
// 				fmt.Println("auth successfull")
// 				return nil
// 			case 5:
// 				salt := make([]byte, 4)
// 				io.ReadFull(conn, salt)
// 				hash := md5HashPassword(user, pass, salt)
// 				sendPasswordMessage(conn, hash)
// 			default:
// 				return fmt.Errorf("unsupported auth method: %d", authType)
// 			}
// 		case 'E':
// 			return fmt.Errorf("error occurred during auth")
// 		default:
// 			return fmt.Errorf("unexpected message type during auth: %c", msgType)
// 		}
// 	}
// }

// func sendPasswordMessage(conn net.Conn, hashed string) {
// 	buf := new(bytes.Buffer)
// 	writeCString(buf, hashed)
// 	msg := buf.Bytes()

// 	packet := new(bytes.Buffer)
// 	packet.WriteByte('p')
// 	binary.Write(packet, binary.BigEndian, int32(len(msg)+4))
// 	packet.Write(msg)

// 	conn.Write(packet.Bytes())
// }

// func md5HashPassword(user, pass string, salt []byte) string {
// 	h1 := md5.Sum([]byte(pass + user))
// 	h1Hex := fmt.Sprintf("%x", h1[:])

// 	h2 := md5.New()
// 	h2.Write([]byte(h1Hex))
// 	h2.Write(salt)
// 	sum := h2.Sum(nil)

// 	return "md5" + hex.EncodeToString(sum)
// }

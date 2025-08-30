package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"pgclient/client"
	"strings"
)

const (
	host     = "192.168.0.25"
	port     = "5433"
	username = "mislavclient"
	database = "postgres"
	password = "postgres"
)

// Host=192.168.0.25;Port=5433;Database=postgres;Username=mislavclient;Password=postgres;

func main() {
	connDetails, _ := parseArguments()
	// query := "explain analyze SELECT * FROM \"TestTable\";"
	query := "insert into \"TestTable\" (\"Id\", \"Name\", \"Age\") values (gen_random_uuid(), 'Jane Doe', 99);"
	fmt.Println("Query to execute:", query)

	pgConn := client.NewPgConnection(connDetails)
	err := pgConn.Connect()
	if err != nil {
		panic(err)
	}
	defer pgConn.Close()

	go func() {
		err = pgConn.SendQuery("SELECT * FROM \"TestTable\";")
		if err != nil {
			panic(err)
		}
		fmt.Println(pgConn.TransactionStatus)

		values2 := pgConn.ReadQueryResponse()
		fmt.Println(len(values2), "rows received")
		for _, value := range values2 {
			fmt.Println("Value:", value)
		}
	}()

	err = pgConn.SendQuery(query)
	if err != nil {
		panic(err)
	}
	fmt.Println(pgConn.TransactionStatus)

	values := pgConn.ReadQueryResponse()
	fmt.Println(len(values), "rows received")
	for _, value := range values {
		fmt.Println("Value:", value)
	}
}

func parseArguments() (client.ConnectionDetails, string) {
	connStringArgument := flag.String("c", "", "Connection string")
	query := flag.String("q", "", "SQL query to execute")
	flag.Parse()
	connDetails := parseConnectionString(*connStringArgument)
	fmt.Printf("%+v\n", connDetails)

	return connDetails, *query
}

func parseConnectionString(connString string) client.ConnectionDetails {
	fmt.Println(connString)
	split := strings.Split(connString, ";")
	fmt.Println(split)
	details := client.ConnectionDetails{}

	assignMap := map[string]*string{
		"host":     &details.Host,
		"port":     &details.Port,
		"username": &details.Username,
		"password": &details.Password,
		"database": &details.Database,
	}

	for _, part := range split {
		if part == "" {
			continue
		}
		item := strings.SplitN(part, "=", 2)
		if len(item) != 2 {
			panic(fmt.Errorf("there was an issue parsing %s", item[0]))
		}
		key := strings.ToLower(strings.TrimSpace(item[0]))
		value := strings.TrimSpace(item[1])

		if ptr, ok := assignMap[key]; ok {
			*ptr = value
		}
	}

	return details
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

func md5HashPassword(user, pass string, salt []byte) string {
	h1 := md5.Sum([]byte(pass + user))
	h1Hex := fmt.Sprintf("%x", h1[:])

	h2 := md5.New()
	h2.Write([]byte(h1Hex))
	h2.Write(salt)
	sum := h2.Sum(nil)

	return "md5" + hex.EncodeToString(sum)
}

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"pgclient/message"
	"strings"
)

type RowDescription struct {
	fieldName             string
	tableObjectId         int32
	attributeNumber       int16
	fieldDataTypeObjectId int32
	dataTypeSize          int16
	typeModifier          int32
	formatCode            int16
}

type ConnectionDetails struct {
	host     string
	port     string
	username string
	password string
	database string
}

const (
	host     = "192.168.0.25"
	port     = "5433"
	username = "mislavclient"
	database = "postgres"
	password = "postgres"
)

func main() {
	connDetails, _ := parseArguments()

	pgConn := NewPgConnection(connDetails)
	err := pgConn.Connect()
	if err != nil {
		panic(err)
	}
	defer pgConn.Close()
	// host_name := connDetails.host + ":" + string(connDetails.port)
	// fmt.Printf("Starting connection to server(%s)...\n", host_name)

	// client := NewTCPClient(connDetails.host, connDetails.port)
	// err := client.ConnectToServer()
	// if err != nil {
	// 	fmt.Printf("Error while connecting to server: %s\n", err)
	// 	return
	// }

	// fmt.Printf("%s %s\n", client.conn.LocalAddr(), client.conn.RemoteAddr())

	// writer := message.NewPgWriter(client.conn)
	// // reader := message.NewPgReader(client.conn)

	// //write bytes to the remote server
	// err = send_startup_message(writer, connDetails.username, connDetails.database)
	// if err != nil {
	// 	fmt.Println(err)
	// 	panic(err)
	// }

	// //fmt.Println("Starting auth...")
	// //err = handleAuthentication(conn, username, password)
	// //if err != nil {
	// //	panic(err)
	// //}

	// fmt.Println("[Main] Starting waitForReady loop...")
	// waitForReady(client.conn)

	// sendQuery(client.conn, query)
	// readQueryResponse(client.conn)
}

func parseArguments() (ConnectionDetails, string) {
	connStringArgument := flag.String("c", "", "Connection string")
	query := flag.String("q", "", "SQL query to execute")
	flag.Parse()
	connDetails := parseConnectionString(*connStringArgument)
	fmt.Printf("%+v\n", connDetails)

	return connDetails, *query
}

func parseConnectionString(connString string) ConnectionDetails {
	fmt.Println(connString)
	split := strings.Split(connString, ";")
	fmt.Println(split)
	details := ConnectionDetails{}

	assignMap := map[string]*string{
		"host":     &details.host,
		"port":     &details.port,
		"username": &details.username,
		"password": &details.password,
		"database": &details.database,
	}

	for _, part := range split {
		if part == "" {
			continue
		}
		item := strings.SplitN(part, "=", 2)
		if len(item) != 2 {
			panic(fmt.Errorf("There was an issue parsing %s", item[0]))
		}
		key := strings.ToLower(strings.TrimSpace(item[0]))
		value := strings.TrimSpace(item[1])

		if ptr, ok := assignMap[key]; ok {
			*ptr = value
		}
	}

	return details
}

func readQueryResponse(conn net.Conn) {
	fields := make([]RowDescription, 0)
	for {
		msgType := readByte(conn)
		fmt.Printf("[ReadQueryResponse] Received message of type: %s\n", string(msgType))
		length := readInt32(conn)
		fmt.Printf("[ReadQueryResponse] Length of the message: %d\n", length)
		switch msgType {
		case 'T':
			fmt.Printf("[ReadQueryResponse] Recevied RowDescription message.\n")
			noOfFields := readInt16(conn)
			fmt.Printf("no of fileds: %d\n\n\n", noOfFields)

			i := 0
			for i < int(noOfFields) {
				row := new(RowDescription)

				fieldName, _ := readCString(conn)
				row.fieldName = fieldName
				row.tableObjectId = readInt32(conn)
				row.attributeNumber = readInt16(conn)
				row.fieldDataTypeObjectId = readInt32(conn)
				row.dataTypeSize = readInt16(conn)
				row.typeModifier = readInt32(conn)
				row.formatCode = readInt16(conn)

				fields = append(fields, *row)
				i++
			}
			fmt.Println(fields)
		case 'D':
			noOfFields := readInt16(conn)

			i := 0
			data := make(map[string]any)
			for i < int(noOfFields) {
				valueLength := readInt32(conn)
				if valueLength < 0 { //-1 indicates a NULL column value
					data[fields[i].fieldName] = nil
				} else {
					var value any
					fmt.Printf("Format code value: %d\n", fields[i].formatCode)
					if fields[i].formatCode == 0 {
						//text format
						valueBytes := readNBytes(conn, int(valueLength))
						value = string(valueBytes)
					} else {
						value = readNBytes(conn, int(valueLength))
					}
					//need to know the column type to map the data returned by the server
					data[fields[i].fieldName] = value
				}
				i++
			}
			for key, value := range data {
				fmt.Println("key:", key, "Value:", value)
			}
		case 'C':
			commandTag, _ := readCString(conn)
			fmt.Printf("[ReadQueryResponse] Recevied following command tag: %s\n", commandTag)
		case 'E':
			code := string(readByte(conn))
			field, _ := readCString(conn)
			fmt.Printf("[ReadQueryResponse] Error: %s %s\n", code, field)
		}
	}
}

func sendQuery(conn net.Conn, query string) {
	var q string
	if query != "" {
		q = query
	} else {
		q = "SELECT '1' as AgencyId, '175' as \"UserId\";"
	}

	buf := new(bytes.Buffer)
	//writeCString(buf, "Q")
	buf.WriteByte('Q')

	binary.Write(buf, binary.BigEndian, int32(len(q)+4+1))

	writeCString(buf, q)
	//buf.WriteByte(0)

	conn.Write(buf.Bytes())
}

// this ones should be broken into smaller pieces, probably per message type
func waitForReady(conn net.Conn) {

	for {
		msgType := readByte(conn)
		converted := string(msgType)
		fmt.Printf("[WaitForReady] Server response: %s\n", converted)
		length := readInt32(conn)
		fmt.Printf("[WaitForReady] Message length: %d\n", length)
		switch msgType {
		case 'Z': //waitForReady
			status := readByte(conn) //transaction status
			fmt.Printf("[WaitForReady] ReadyForQuery received - transaction status: %s\n", string(status))
			return
		case 'S': //ParameterStatus
			//should think about storing these values for future use
			param, err := readCString(conn)
			if err != nil {
				panic(err)
			}
			fmt.Printf("[waitForReady] Received param status message param: %s\n", param)
			value, err2 := readCString(conn)
			if err2 != nil {
				panic(err2)
			}
			fmt.Printf("[WaitForReady] Received param status message value: %s\n", value)
		case 'K': //BackendKeyData
			processId := readInt32(conn)
			fmt.Printf("[WaitForReady] Received backend PID: %d\n", processId)
			secretKey := readInt32(conn)
			fmt.Printf("[WaitForReady] Recevied secret key: %d\n", secretKey)
		case 'E': //error response
			fmt.Println("[WaitForReady] Server returned error.")
			return
		default:
			skipN(conn, length-4)
			fmt.Println("[WaitForReady] Found default message type.")
		}
	}
}

func send_startup_message(writer *message.PgWriter, user, db string) error {
	buf := new(bytes.Buffer)
	//protocol version number 3.0 - 196608
	binary.Write(buf, binary.BigEndian, int32(196608))

	writeCString(buf, "user")
	writeCString(buf, user)

	writeCString(buf, "database")
	writeCString(buf, db)

	writeCString(buf, "application_name")
	writeCString(buf, "PgClient")

	buf.WriteByte(0)

	msg := buf.Bytes()
	final := new(bytes.Buffer)
	binary.Write(final, binary.BigEndian, int32(len(msg)+4))
	final.Write(msg)

	_, err := writer.Write(final.Bytes())
	// client.Send(final.Bytes())
	return err
}

// func send_startup_message(conn net.Conn, user, db string) error {
// 	buf := new(bytes.Buffer)
// 	//protocol version number 3.0 - 196608
// 	binary.Write(buf, binary.BigEndian, int32(196608))

// 	writeCString(buf, "user")
// 	writeCString(buf, user)

// 	writeCString(buf, "database")
// 	writeCString(buf, db)

// 	writeCString(buf, "application_name")
// 	writeCString(buf, "PgClient")

// 	buf.WriteByte(0)

// 	msg := buf.Bytes()
// 	final := new(bytes.Buffer)
// 	binary.Write(final, binary.BigEndian, int32(len(msg)+4))
// 	final.Write(msg)

// 	response, err := conn.Write(final.Bytes())
// 	fmt.Println("[StartUp] Server response: ")
// 	fmt.Println(string(response))
// 	return err
// }

func readInt16(conn net.Conn) int16 {
	var b [2]byte
	io.ReadFull(conn, b[:])
	return int16(binary.BigEndian.Uint16(b[:]))
}

func readInt32(conn net.Conn) int32 {
	var b [4]byte
	io.ReadFull(conn, b[:])
	return int32(binary.BigEndian.Uint32(b[:]))
}

func readNBytes(conn net.Conn, n int) []byte {
	b := make([]byte, n)
	io.ReadFull(conn, b[:])
	return b
}

func writeCString(buf *bytes.Buffer, s string) {
	buf.WriteString(s)
	buf.WriteByte(0)
}

// reads byte by byte unitl hitting null terminator
func readCString(conn net.Conn) (string, error) {
	var buf bytes.Buffer
	one := make([]byte, 1)

	for {
		_, err := conn.Read(one)
		if err != nil {
			return "", err
		}
		if one[0] == 0 {
			break
		}
		buf.WriteByte(one[0])
	}
	return buf.String(), nil
}

func handleAuthentication(conn net.Conn, user, pass string) error {
	for {
		msgType := readByte(conn)
		//length := readInt32(conn)
		fmt.Print("Message type:")
		fmt.Println(msgType)
		switch msgType {
		case 'R': //authentication
			authType := readInt32(conn)
			switch authType {
			case 0: //OK
				fmt.Println("auth successfull")
				return nil
			case 5:
				salt := make([]byte, 4)
				io.ReadFull(conn, salt)
				hash := md5HashPassword(user, pass, salt)
				sendPasswordMessage(conn, hash)
			default:
				return fmt.Errorf("unsupported auth method: %d", authType)
			}
		case 'E':
			return fmt.Errorf("error occurred during auth")
		default:
			return fmt.Errorf("unexpected message type during auth: %c", msgType)
		}
	}
}

func sendPasswordMessage(conn net.Conn, hashed string) {
	buf := new(bytes.Buffer)
	writeCString(buf, hashed)
	msg := buf.Bytes()

	packet := new(bytes.Buffer)
	packet.WriteByte('p')
	binary.Write(packet, binary.BigEndian, int32(len(msg)+4))
	packet.Write(msg)

	conn.Write(packet.Bytes())
}

func md5HashPassword(user, pass string, salt []byte) string {
	h1 := md5.Sum([]byte(pass + user))
	h1Hex := fmt.Sprintf("%x", h1[:])

	h2 := md5.New()
	h2.Write([]byte(h1Hex))
	h2.Write(salt)
	sum := h2.Sum(nil)

	return "md5" + hex.EncodeToString(sum)
}

func readByte(conn net.Conn) byte {
	b := make([]byte, 1)
	_, err := conn.Read(b)
	if err != nil {
		panic(err)
	}
	return b[0]
}

func skipN(conn net.Conn, n int32) {
	io.CopyN(io.Discard, conn, int64(n))
}

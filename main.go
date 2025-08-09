package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
)

const (
	host     = "localhost"
	port     = "5433"
	username = "mislavclient"
	database = "postgres"
	password = "postgres"
)

func main() {
	host_name := host + ":" + string(port)
	fmt.Printf("Starting connection to server(%s)...\n", host_name)

	conn, err := net.Dial("tcp", host_name)
	if err != nil {
		fmt.Printf("Error while connection to server: %s", err)
	}
	defer conn.Close()

	fmt.Printf("%s %s\n", conn.LocalAddr(), conn.RemoteAddr())

	//write bytes to the remote server
	err = send_startup_message(conn, username, database)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	//fmt.Println("Starting auth...")
	//err = handleAuthentication(conn, username, password)
	//if err != nil {
	//	panic(err)
	//}

	fmt.Println("[Main] Starting waitForReady loop...")
	waitForReady(conn)

	sendQuery(conn)
	readQueryResponse(conn)
}

func readQueryResponse(conn net.Conn) {
	for {
		msgType := readByte(conn)
		fmt.Printf("[ReadQueryResponse] Received message of type: %s\n", string(msgType))
		length := readInt32(conn)
		fmt.Printf("[ReadQueryResponse] Length of the message: %d\n", length)
		switch msgType {
		case 'E':
			code := string(readByte(conn))
			field, _ := readCString(conn)
			fmt.Printf("[ReadQueryResponse] Error: %s %s\n", code, field)
		}
	}
}

func sendQuery(conn net.Conn) {
	query := "SELECT 1;"

	buf := new(bytes.Buffer)
	//writeCString(buf, "Q")
	buf.WriteByte('Q')

	binary.Write(buf, binary.BigEndian, int32(len(query)+4+1))

	writeCString(buf, query)
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

func send_startup_message(conn net.Conn, user, db string) error {
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

	response, err := conn.Write(final.Bytes())
	fmt.Println("[StartUp] Server response: ")
	fmt.Println(string(response))
	return err
}

func readInt32(conn net.Conn) int32 {
	var b [4]byte
	io.ReadFull(conn, b[:])
	return int32(binary.BigEndian.Uint32(b[:]))
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

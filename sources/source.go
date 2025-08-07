package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
)

const (
	host     = "localhost"
	port     = 5432
	username = "postgres"
	password = "yourpassword"
	database = "postgres"
)

func main() {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// 1. Send startup message
	err = sendStartupMessage(conn, username, database)
	if err != nil {
		panic(err)
	}

	// 2. Handle authentication request
	err = handleAuthentication(conn, username, password)
	if err != nil {
		panic(err)
	}

	// 3. Wait for ReadyForQuery
	waitForReady(conn)

	// 4. Send simple query
	sendQuery(conn, "SELECT 1")

	// 5. Read response
	readQueryResponse(conn)
}

func sendStartupMessage(conn net.Conn, user, db string) error {
	buf := new(bytes.Buffer)
	// Protocol version 3.0
	binary.Write(buf, binary.BigEndian, int32(196608))

	// Parameters
	writeCString(buf, "user")
	writeCString(buf, user)

	writeCString(buf, "database")
	writeCString(buf, db)

	// Terminator
	buf.WriteByte(0)

	// Message length
	msg := buf.Bytes()
	final := new(bytes.Buffer)
	binary.Write(final, binary.BigEndian, int32(len(msg)+4))
	final.Write(msg)

	_, err := conn.Write(final.Bytes())
	return err
}

func handleAuthentication(conn net.Conn, user, pass string) error {
	for {
		msgType := readByte(conn)
		length := readInt32(conn)

		switch msgType {
		case 'R': // Authentication
			authType := readInt32(conn)
			switch authType {
			case 0: // OK
				fmt.Println("Authentication successful")
				return nil
			case 5: // MD5
				salt := make([]byte, 4)
				io.ReadFull(conn, salt)
				hash := md5HashPassword(user, pass, salt)
				sendPasswordMessage(conn, hash)
			default:
				return fmt.Errorf("unsupported auth method: %d", authType)
			}
		case 'E': // Error
			return fmt.Errorf("server error: %s", readError(conn, length))
		default:
			return fmt.Errorf("unexpected message type during auth: %c", msgType)
		}
	}
}

func md5HashPassword(user, pass string, salt []byte) string {
	// First: md5(password + username)
	h1 := md5.Sum([]byte(pass + user))
	h1Hex := fmt.Sprintf("%x", h1[:])

	// Second: md5(h1 + salt)
	h2 := md5.New()
	h2.Write([]byte(h1Hex))
	h2.Write(salt)
	sum := h2.Sum(nil)

	return "md5" + hex.EncodeToString(sum)
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

func waitForReady(conn net.Conn) {
	for {
		msgType := readByte(conn)
		length := readInt32(conn)
		switch msgType {
		case 'Z':
			_ = readByte(conn) // transaction status
			fmt.Println("ReadyForQuery received")
			return
		default:
			skipN(conn, length-4)
		}
	}
}

func sendQuery(conn net.Conn, query string) {
	buf := new(bytes.Buffer)
	writeCString(buf, query)

	packet := new(bytes.Buffer)
	packet.WriteByte('Q')
	binary.Write(packet, binary.BigEndian, int32(buf.Len()+4))
	packet.Write(buf.Bytes())

	conn.Write(packet.Bytes())
}

func readQueryResponse(conn net.Conn) {
	for {
		msgType := readByte(conn)
		length := readInt32(conn)
		switch msgType {
		case 'T':
			fmt.Println("Row description (not parsed)")
			skipN(conn, length-4)
		case 'D':
			fmt.Println("DataRow:")
			fieldCount := readInt16(conn)
			for i := 0; i < int(fieldCount); i++ {
				fieldLen := readInt32(conn)
				field := make([]byte, fieldLen)
				io.ReadFull(conn, field)
				fmt.Printf("  Field %d: %s\n", i+1, string(field))
			}
		case 'C':
			fmt.Println("Command complete")
			skipN(conn, length-4)
		case 'Z':
			fmt.Println("Ready for next query")
			return
		case 'E':
			fmt.Printf("Error: %s\n", readError(conn, length))
			return
		default:
			skipN(conn, length-4)
		}
	}
}

func writeCString(buf *bytes.Buffer, s string) {
	buf.WriteString(s)
	buf.WriteByte(0)
}

func readByte(conn net.Conn) byte {
	b := make([]byte, 1)
	_, err := conn.Read(b)
	if err != nil {
		panic(err)
	}
	return b[0]
}

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

func skipN(conn net.Conn, n int32) {
	io.CopyN(io.Discard, conn, int64(n))
}

func readError(conn net.Conn, length int32) string {
	buf := make([]byte, length-4)
	io.ReadFull(conn, buf)
	return string(buf)
}

CONNECT to PostgreSQL server (host, port) over TCP

SEND StartupMessage:
    - protocol version (3.0)
    - user name
    - database name
    - other parameters (optional)

LOOP:  // Authentication phase
    RECEIVE message from server
    IF message is AuthenticationRequest:
        IF authentication method is OK:
            BREAK authentication loop
        ELSE IF authentication method is MD5:
            READ salt from server
            COMPUTE MD5 hash of password using salt
            SEND PasswordMessage with hashed password
        ELSE IF authentication method is Cleartext:
            SEND PasswordMessage with plain password
        ELSE:
            ERROR: Unsupported authentication method
    ELSE IF message is ErrorResponse:
        HANDLE error and EXIT

WAIT until ReadyForQuery message received from server

SEND SimpleQuery message with SQL statement (e.g. "SELECT 1")

LOOP:  // Query response phase
    RECEIVE message from server
    IF message is RowDescription:
        PARSE row metadata (field names, types)
    ELSE IF message is DataRow:
        PARSE row data fields
    ELSE IF message is CommandComplete:
        NOTE command completed
    ELSE IF message is ReadyForQuery:
        QUERY finished, break loop
    ELSE IF message is ErrorResponse:
        HANDLE error and EXIT

CLOSE connection or send more queries as needed

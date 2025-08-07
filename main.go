package main

import (
	"fmt"
	"net"
	"bytes"
	"encoding/binary"
	"io"
)

const (
	host = "localhost"
	port = "5433"
	username = "mislavsu"
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
	fmt.Println("[Main] Starting waitForReady loop...")
	waitForReady(conn)
}

func waitForReady(conn net.Conn) {
	for {
		msgType := readByte(conn)
		length := readInt32(conn)
		switch msgType {
			case 'Z':
				_ = readByte(conn) //transaction status
				fmt.Println("[waitForReady] ReadtForQuery received")
				return
			default:
				skipN(conn, length-4)
		}
	}
}

func send_startup_message(conn net.Conn, user, db string) error {
	buf := new(bytes.Buffer)
	//protocol version number 3.0 - 196608
	binary.Write(buf, binary.BigEndian, int32(196608))
	
	writeCString(buf, "user")
	writeCString(buf, username)

	writeCString(buf, "database")
	writeCString(buf, database)

	buf.WriteByte(0)

	msg := buf.Bytes()
	final := new(bytes.Buffer)
	binary.Write(final, binary.BigEndian, int32(len(msg)+4))
	final.Write(msg)

	response, err := conn.Write(final.Bytes())
	fmt.Println("[StartUp] Server response: ")
	fmt.Println(response)
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

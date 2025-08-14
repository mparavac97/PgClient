package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"pgclient/message"
)

type PgConnection struct {
	details  ConnectionDetails
	writer   *message.PgWriter
	reader   *message.PgReader
	client   *TCPClient
	handlers map[byte]message.ResponseHandler
	params   map[string]string
}

func NewPgConnection(details ConnectionDetails) *PgConnection {
	client := NewTCPClient(details.host, details.port)

	return &PgConnection{
		details:  details,
		client:   client,
		handlers: message.InitializeHandlers(),
		params:   make(map[string]string),
	}
}

func (conn *PgConnection) Connect() error {
	fmt.Println("Connecting to server...")
	err := conn.client.ConnectToServer()
	if err != nil {
		return err
	}

	// Initialize writer and reader AFTER the connection exists
	conn.writer = message.NewPgWriter(conn.client.conn)
	conn.reader = message.NewPgReader(conn.client.conn)

	fmt.Println("Connected to server.")
	fmt.Println("Sending startup message...")
	err = conn.sendStartupMessage()
	if err != nil {
		return err
	}

	for {
		msgType, err := conn.reader.ReadByte()
		if err != nil {
			return fmt.Errorf("error reading message type: %w", err)
		}
		fmt.Println("Received message type:", msgType)
		length, err := conn.reader.ReadInt32()
		if err != nil {
			return fmt.Errorf("error reading message type: %w", err)
		}
		fmt.Println(length, "bytes in message")
		handler := conn.handlers[msgType]
		if handler == nil {
			fmt.Printf("No handler for message type: %c\n", msgType)
			continue
		}
		value, err := handler(message.MessageType(msgType), nil)
	}

	return nil
}

func (conn *PgConnection) Close() error {
	if conn.client != nil {
		return conn.client.Close()
	}
	return nil
}

func (conn *PgConnection) sendStartupMessage() error {
	buf := new(bytes.Buffer)
	//protocol version number 3.0 - 196608
	binary.Write(buf, binary.BigEndian, int32(196608))

	writeCString(buf, "user")
	writeCString(buf, conn.details.username)

	writeCString(buf, "database")
	writeCString(buf, conn.details.database)

	writeCString(buf, "application_name")
	writeCString(buf, "PgClient")

	buf.WriteByte(0)

	msg := buf.Bytes()
	final := new(bytes.Buffer)
	binary.Write(final, binary.BigEndian, int32(len(msg)+4))
	final.Write(msg)

	if conn.writer == nil {
		return fmt.Errorf("writer is not initialized")
	} else {
		_, err := conn.writer.Write(final.Bytes())
		return err
	}

}

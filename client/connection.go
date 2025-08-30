package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"pgclient/message"
)

type ConnectionDetails struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

type RowDescription struct {
	fieldName             string
	tableObjectId         int32
	attributeNumber       int16
	fieldDataTypeObjectId int32
	dataTypeSize          int16
	typeModifier          int32
	formatCode            int16
}

type PgConnection struct {
	details           ConnectionDetails
	writer            *message.PgWriter
	reader            *message.PgReader
	client            *TCPClient
	params            map[string]string
	TransactionStatus string
}

func NewPgConnection(details ConnectionDetails) *PgConnection {
	client := NewTCPClient(details.Host, details.Port)

	return &PgConnection{
		details: details,
		client:  client,
		params:  make(map[string]string),
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

	//wait for the server to respond with messages, until it sends ReadyForQuery
	for {
		msgType, err := conn.reader.ReadByte()
		if err != nil {
			return fmt.Errorf("error reading message type: %w", err)
		}
		fmt.Println("Received message type:", string(msgType))
		length, err := conn.reader.ReadInt32()
		if err != nil {
			return fmt.Errorf("error reading message type: %w", err)
		}
		fmt.Println(length, "bytes in message")

		switch msgType {
		case byte(message.ParameterStatus):
			param, value, err := message.ProcessParameterStatus(conn.reader)
			if err != nil {
				return fmt.Errorf("error processing parameter status: %w", err)
			}
			conn.params[param] = value
		case byte(message.BackendKeyData):
			pid, key, err := message.ProcessBackendKeyData(conn.reader)
			if err != nil {
				return fmt.Errorf("error processing backend key data: %w", err)
			}
			conn.params["pid"] = fmt.Sprintf("%d", pid)
			conn.params["key"] = fmt.Sprintf("%d", key)
		case byte(message.ReadyForQuery):
			status, err := message.ProcessReadyForQuery(conn.reader)
			if err != nil {
				return fmt.Errorf("error processing ready for query: %w", err)
			}
			conn.TransactionStatus = status
			return nil
		default:
			conn.reader.SkipN(length - 4)
			fmt.Println("Found default message type.")
		}
	}
}

func (conn *PgConnection) Close() error {
	if conn.client != nil {
		return conn.client.Close()
	}
	return nil
}

func (conn *PgConnection) SendQuery(query string) error {
	buf := new(bytes.Buffer)
	buf.WriteByte('Q')

	binary.Write(buf, binary.BigEndian, int32(len(query)+5))
	conn.writer.WriteCString(buf, query)
	fmt.Println("Sending query:", query)
	_, err := conn.writer.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("error writing query: %w", err)
	}

	return nil
}

func (conn *PgConnection) sendStartupMessage() error {
	buf := new(bytes.Buffer)
	//protocol version number 3.0 - 196608
	binary.Write(buf, binary.BigEndian, int32(196608))

	conn.writer.WriteCString(buf, "user")
	conn.writer.WriteCString(buf, conn.details.Username)

	conn.writer.WriteCString(buf, "database")
	conn.writer.WriteCString(buf, conn.details.Database)

	conn.writer.WriteCString(buf, "application_name")
	conn.writer.WriteCString(buf, "PgClient")

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

func (conn *PgConnection) ReadQueryResponse() []map[string]any {
	fields := make([]RowDescription, 0)
	rows := make([]map[string]any, 0)
	for {
		msgType, err := conn.reader.ReadByte()
		if err != nil {
			fmt.Println("error reading message type:", err)
			return nil
		}
		fmt.Printf("[ReadQueryResponse] Received message of type: %s\n", string(msgType))
		length, err := conn.reader.ReadInt32()
		if err != nil {
			fmt.Println("error reading message length:", err)
			return nil
		}

		fmt.Printf("[ReadQueryResponse] Length of the message: %d\n", length)
		switch msgType {
		case byte(message.RowDescription):
			fmt.Printf("[ReadQueryResponse] Recevied RowDescription message.\n")
			noOfFields, err := conn.reader.ReadInt16()
			if err != nil {
				fmt.Println("error reading no of fields:", err)
				return nil
			}
			fmt.Printf("no of fileds: %d\n\n\n", noOfFields)

			i := 0
			for i < int(noOfFields) {
				row := new(RowDescription)

				fieldName, _ := conn.reader.ReadCString()
				row.fieldName = fieldName
				row.tableObjectId, _ = conn.reader.ReadInt32()
				row.attributeNumber, _ = conn.reader.ReadInt16()
				row.fieldDataTypeObjectId, _ = conn.reader.ReadInt32()
				row.dataTypeSize, _ = conn.reader.ReadInt16()
				row.typeModifier, _ = conn.reader.ReadInt32()
				row.formatCode, _ = conn.reader.ReadInt16()

				fields = append(fields, *row)
				i++
			}
			fmt.Println(fields)
		case byte(message.DataRow):
			noOfFields, err := conn.reader.ReadInt16()
			if err != nil {
				fmt.Println("error reading no of fields in data row:", err)
				return nil
			}

			i := 0
			data := make(map[string]any)
			for i < int(noOfFields) {
				valueLength, err := conn.reader.ReadInt32()
				if err != nil {
					fmt.Println("error reading value length:", err)
					return nil
				}
				if valueLength < 0 { //-1 indicates a NULL column value
					data[fields[i].fieldName] = nil
				} else {
					var value any
					if fields[i].formatCode == 0 {
						//text format
						valueBytes := conn.reader.ReadNBytes(int(valueLength))
						value = string(valueBytes)
					} else {
						value = conn.reader.ReadNBytes(int(valueLength))
					}
					//need to know the column type to map the data returned by the server
					data[fields[i].fieldName] = value
				}
				i++
			}
			rows = append(rows, data)
		case 'C':
			commandTag, _ := conn.reader.ReadCString()
			fmt.Printf("[ReadQueryResponse] Recevied following command tag: %s\n", commandTag)
		case byte(message.ReadyForQuery):
			status, err := message.ProcessReadyForQuery(conn.reader)
			if err != nil {
				fmt.Println("error processing ready for query:", err)
				return nil
			}
			conn.TransactionStatus = status
			return rows

		case 'E':
			code, err := conn.reader.ReadByte()
			if err != nil {
				fmt.Println("error reading error code:", err)
				return nil
			}
			field, _ := conn.reader.ReadCString()
			fmt.Printf("[ReadQueryResponse] Error: %s %s\n", string(code), field)
		}
	}
}

func WaitForReadyForQuery(conn *PgConnection) error {
	//wait for the server to respond with messages, until it sends ReadyForQuery
	for {
		msgType, err := conn.reader.ReadByte()
		if err != nil {
			return fmt.Errorf("error reading message type: %w", err)
		}
		fmt.Println("Received message type:", string(msgType))
		length, err := conn.reader.ReadInt32()
		if err != nil {
			return fmt.Errorf("error reading message type: %w", err)
		}
		fmt.Println(length, "bytes in message")

		switch msgType {
		case byte(message.ParameterStatus):
			param, value, err := message.ProcessParameterStatus(conn.reader)
			if err != nil {
				return fmt.Errorf("error processing parameter status: %w", err)
			}
			conn.params[param] = value
		case byte(message.BackendKeyData):
			pid, key, err := message.ProcessBackendKeyData(conn.reader)
			if err != nil {
				return fmt.Errorf("error processing backend key data: %w", err)
			}
			conn.params["pid"] = fmt.Sprintf("%d", pid)
			conn.params["key"] = fmt.Sprintf("%d", key)
		case byte(message.ReadyForQuery):
			status, err := message.ProcessReadyForQuery(conn.reader)
			if err != nil {
				return fmt.Errorf("error processing ready for query: %w", err)
			}
			conn.TransactionStatus = status
			return nil
		default:
			conn.reader.SkipN(length - 4)
			fmt.Println("Found default message type.")
		}
	}
}

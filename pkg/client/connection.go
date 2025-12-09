package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mparavac97/PgClient/pkg/message"
)

type QueryRequest struct {
	query      string
	params     map[string]any
	paramNames []string
	result     chan QueryResult
}

type QueryResult struct {
	Rows []map[string]any
	err  error
}

type ConnectionDetails struct {
	Host              string
	Port              string
	Username          string
	Password          string
	Database          string
	ConnectionTimeout string // in seconds
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
	connParams        map[string]string
	TransactionStatus string
	queryQueue        chan QueryRequest
}

const (
	ErrorSeverity = 'S'
	ErrorCode     = 'C'
	ErrorMessage  = 'M'
	ErrorDetail   = 'D'
	ErrorHint     = 'H'
	ErrorPosition = 'P'
	ErrorWhere    = 'W'
	ErrorFile     = 'F'
	ErrorLine     = 'L'
	ErrorRoutine  = 'R'
)

func NewPgConnection(connectionString string) *PgConnection {
	details := parseConnectionString(connectionString)
	client := NewTCPClient(details.Host, details.Port)

	conn := &PgConnection{
		details:    details,
		client:     client,
		connParams: make(map[string]string),
		queryQueue: make(chan QueryRequest, 100), // buffered channel to hold up to 100 queries
	}

	go conn.ProcessQueries()
	return conn
}

func (conn *PgConnection) Connect() error {
	fmt.Println("Connecting to server...")

	// Create a context with timeout or use background context for no timeout
	var ctx context.Context
	var cancel context.CancelFunc

	timeout, err := strconv.Atoi(conn.details.ConnectionTimeout)
	if err != nil || timeout <= 0 {
		// Use background context with no timeout
		ctx = context.Background()
		cancel = func() {} // Empty cancel function
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	}
	defer cancel()

	done := make(chan error, 1)

	// Start connection process in goroutine
	go func() {
		if err := conn.client.ConnectToServer(ctx); err != nil {
			done <- err
			return
		}

		// Initialize writer and reader AFTER the connection exists
		conn.writer = message.NewPgWriter(conn.client.conn)
		conn.reader = message.NewPgReader(conn.client.conn)

		fmt.Println("Connected to server.")
		fmt.Println("Sending startup message...")
		if err = conn.sendStartupMessage(); err != nil {
			done <- err
			return
		}

		// Handle server messages until ReadyForQuery
		for {
			msgType, err := conn.reader.ReadByte()
			if err != nil {
				done <- fmt.Errorf("error reading message type: %w", err)
				return
			}
			fmt.Println("Received message type:", message.MessageType(msgType).String())
			length, err := conn.reader.ReadInt32()
			if err != nil {
				done <- fmt.Errorf("error reading message type: %w", err)
				return
			}

			switch msgType {
			case byte(message.ParameterStatus):
				param, value, err := message.ProcessParameterStatus(conn.reader)
				if err != nil {
					done <- fmt.Errorf("error processing parameter status: %w", err)
					return
				}
				conn.connParams[param] = value
			case byte(message.BackendKeyData):
				pid, key, err := message.ProcessBackendKeyData(conn.reader)
				if err != nil {
					done <- fmt.Errorf("error processing backend key data: %w", err)
					return
				}
				conn.connParams["pid"] = fmt.Sprintf("%d", pid)
				conn.connParams["key"] = fmt.Sprintf("%d", key)
			case byte(message.ReadyForQuery):
				status, err := message.ProcessReadyForQuery(conn.reader)
				if err != nil {
					done <- fmt.Errorf("error processing ready for query: %w", err)
					return
				}
				conn.TransactionStatus = status
				done <- nil
				return
			case byte(message.ErrorResponse):
				errResponse, err := message.ProcessErrorResponse(conn.reader, length)
				if err != nil {
					done <- fmt.Errorf("error processing error response: %w", err)
				}
				fmt.Println("server returned error response: ", errResponse)
			default:
				conn.reader.SkipN(length - 4)
				fmt.Println("Found default message type.")
			}
		}
	}()

	// Wait for either completion or timeout
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Clean up connection if it exists
		if conn.client != nil {
			conn.client.Close()
		}
		return fmt.Errorf("connection timeout after %d seconds", timeout)
	}
}

func (conn *PgConnection) Close() error {
	if conn.client != nil {
		return conn.client.Close()
	}
	return nil
}

func (conn *PgConnection) sendQuery(query string, params map[string]any, paramNames []string) error {
	buf := new(bytes.Buffer)
	if len(params) == 0 {
		buf.WriteByte(byte(message.Query))
		binary.Write(buf, binary.BigEndian, int32(len(query)+5))
		conn.writer.WriteCString(buf, query)
		fmt.Println("Sending query:", query)
		_, err := conn.writer.Write(buf.Bytes())
		return err
	} else {
		// extended query protocol
		// 1. Parse message
		parseBuf := new(bytes.Buffer)
		conn.writer.WriteCString(parseBuf, "")                       // unnamed statement
		conn.writer.WriteCString(parseBuf, query)                    // query string
		binary.Write(parseBuf, binary.BigEndian, int16(len(params))) // number of parameter types
		for range params {
			binary.Write(parseBuf, binary.BigEndian, int32(0)) // parameter type OID (0 = unspecified)
		}

		// Write Parse message header
		buf.WriteByte('P')                                           // Parse message type
		binary.Write(buf, binary.BigEndian, int32(parseBuf.Len()+4)) // message length including itself
		buf.Write(parseBuf.Bytes())

		// _, err := conn.writer.Write(buf.Bytes())
		// if err != nil {
		// 	return err
		// }

		// 2. Describe message - describes the prepared statement
		buf.WriteByte('D') // Describe message type
		describeBuf := new(bytes.Buffer)
		describeBuf.WriteByte('S')                // Describe a prepared statement ('S'), not a portal ('P')
		conn.writer.WriteCString(describeBuf, "") // unnamed statement
		binary.Write(buf, binary.BigEndian, int32(describeBuf.Len()+4))
		buf.Write(describeBuf.Bytes())

		// 3. Bind
		buf.WriteByte('B')
		bindInner := new(bytes.Buffer)
		conn.writer.WriteCString(bindInner, "") // unnamed portal
		conn.writer.WriteCString(bindInner, "") // unnamed prepared statement

		// Format codes for parameters (use text format)
		binary.Write(bindInner, binary.BigEndian, int16(len(params)))
		for range params {
			binary.Write(bindInner, binary.BigEndian, int16(0))
		}

		// Parameter values
		binary.Write(bindInner, binary.BigEndian, int16(len(params)))
		for _, param := range paramNames {
			paramStr := fmt.Sprintf("%v", params[param])
			binary.Write(bindInner, binary.BigEndian, int32(len(paramStr)))
			bindInner.WriteString(paramStr)
		}

		// Result format codes (use text format for all)
		binary.Write(bindInner, binary.BigEndian, int16(0))

		binary.Write(buf, binary.BigEndian, int32(bindInner.Len()+4))
		buf.Write(bindInner.Bytes())

		// 4. Execute
		buf.WriteByte('E')
		binary.Write(buf, binary.BigEndian, int32(9)) // message length
		conn.writer.WriteCString(buf, "")             // unnamed portal
		binary.Write(buf, binary.BigEndian, int32(0)) // unlimited rows

		// 5. Sync
		buf.WriteByte('S')
		binary.Write(buf, binary.BigEndian, int32(4))

		_, err := conn.writer.Write(buf.Bytes())
		if err != nil {
			return fmt.Errorf("error writing messages: %w", err)
		}
		return nil
	}
}

func (conn *PgConnection) ProcessQueries() {
	for req := range conn.queryQueue {
		err := conn.sendQuery(req.query, req.params, req.paramNames)
		if err != nil {
			req.result <- QueryResult{nil, err}
			continue
		}

		// Read the response
		rows := conn.readQueryResponse()
		req.result <- QueryResult{rows, nil}
	}
}

func (conn *PgConnection) sendStartupMessage() error {
	buf := new(bytes.Buffer)
	// protocol version number 3.0 - 196608
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

func parseConnectionString(connString string) ConnectionDetails {
	fmt.Println(connString)
	split := strings.Split(connString, ";")
	fmt.Println(split)
	details := ConnectionDetails{}

	assignMap := map[string]*string{
		"host":              &details.Host,
		"port":              &details.Port,
		"username":          &details.Username,
		"password":          &details.Password,
		"database":          &details.Database,
		"connectiontimeout": &details.ConnectionTimeout,
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

func (conn *PgConnection) readQueryResponse() []map[string]any {
	fields := make([]RowDescription, 0)
	rows := make([]map[string]any, 0)
	for {
		msgType, err := conn.reader.ReadByte()
		if err != nil {
			fmt.Println("error reading message type:", err)
			return nil
		}
		fmt.Printf("[ReadQueryResponse] Received message of type: %s\n", message.MessageType(msgType).String())
		_, err = conn.reader.ReadInt32()
		if err != nil {
			fmt.Println("error reading message length:", err)
			return nil
		}
		switch msgType {
		case byte(message.ParameterDescription):
			paramCount, err := conn.reader.ReadInt16()
			if err != nil {
				fmt.Println("error reading parameter count:", err)
				return nil
			}
			// Read parameter type OIDs
			for i := 0; i < int(paramCount); i++ {
				oid, err := conn.reader.ReadInt32() // parameter type OID
				if err != nil {
					fmt.Println("error reading parameter type:", err)
					return nil
				}
				fmt.Println("Parameter", i, "type OID:", oid)
			}
		case byte(message.RowDescription):
			noOfFields, err := conn.reader.ReadInt16()
			if err != nil {
				fmt.Println("error reading no of fields:", err)
				return nil
			}

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
						// text format
						valueBytes := conn.reader.ReadNBytes(int(valueLength))
						value = string(valueBytes)
					} else {
						value = conn.reader.ReadNBytes(int(valueLength))
					}
					// need to know the column type to map the data returned by the server
					data[fields[i].fieldName] = value
				}
				i++
			}
			rows = append(rows, data)
		case byte(message.CommandComplete):
			commandTag, _ := conn.reader.ReadCString()
			fmt.Printf("[ReadQueryResponse] Recevied following command tag: %s\n", commandTag)
		case byte(message.ParseComplete):
			fmt.Println("Parse complete")
		case byte(message.BindComplete):
			fmt.Println("Bind complete")
		case byte(message.ReadyForQuery):
			status, err := message.ProcessReadyForQuery(conn.reader)
			if err != nil {
				fmt.Println("error processing ready for query:", err)
				return nil
			}
			conn.TransactionStatus = status
			return rows
		case byte(message.NoticeResponse):
			for {
				code, err := conn.reader.ReadByte()
				if err != nil {
					fmt.Println("error reading NoticeResponse field code:", err)
					return nil
				}
				if code == 0 {
					// end of message
					break
				}

				value, err := conn.reader.ReadCString()
				if err != nil {
					fmt.Println("error reading NoticeResponse CString:", err)
					return nil
				}

				fmt.Printf("NoticeResponse field: %c => %s\n", code, value)
			}
		case byte(message.FunctionCallResponse):
			fmt.Println("FunctionCallResponse - starting length read.")
			funcResponseLength, err := conn.reader.ReadInt32()
			if err != nil {
				fmt.Println("error processing FunctionCallResponse: ", err)
				return nil
			}
			fmt.Println("FunctionCallResponse - length value: ", funcResponseLength)
			fmt.Println("FunctionCallResponse - starting function result read.")
			x := conn.reader.ReadNBytes(int(funcResponseLength))
			fmt.Println("FunctionCallResponse: ", string(x))
		case byte(message.ErrorResponse):
			errorFields := make(map[byte]string)

			for {
				// Read field type
				fieldType, err := conn.reader.ReadByte()
				if err != nil {
					fmt.Println("error reading field type:", err)
					return nil
				}

				// Check for message terminator
				if fieldType == 0 {
					break
				}

				// Read field value
				fieldValue, err := conn.reader.ReadCString()
				if err != nil {
					fmt.Println("error reading field value:", err)
					return nil
				}

				errorFields[fieldType] = fieldValue
			}

			// Log the complete error
			fmt.Printf("[ReadQueryResponse] PostgreSQL Error:\n")
			fmt.Printf("Severity: %s\n", errorFields[ErrorSeverity])
			fmt.Printf("Code: %s\n", errorFields[ErrorCode])
			fmt.Printf("Message: %s\n", errorFields[ErrorMessage])
			if detail, ok := errorFields[ErrorDetail]; ok {
				fmt.Printf("Detail: %s\n", detail)
			}

			return nil
		}
	}
}

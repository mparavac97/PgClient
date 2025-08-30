package message

import (
	"fmt"
)

type MessageType byte

type ResponseHandler func(messageType MessageType, data []byte) (any, error)

const (
	// Common PostgreSQL message types
	AuthenticationOK     MessageType = 'R'
	BackendKeyData       MessageType = 'K'
	BindComplete         MessageType = '2'
	CloseComplete        MessageType = '3'
	CommandComplete      MessageType = 'C'
	DataRow              MessageType = 'D'
	EmptyQueryResponse   MessageType = 'I'
	ErrorResponse        MessageType = 'E'
	FunctionCallResponse MessageType = 'V'
	NoData               MessageType = 'n'
	NoticeResponse       MessageType = 'N'
	NotificationResponse MessageType = 'A'
	ParameterDescription MessageType = 't'
	ParameterStatus      MessageType = 'S'
	ParseComplete        MessageType = '1'
	PortalSuspended      MessageType = 's'
	ReadyForQuery        MessageType = 'Z'
	RowDescription       MessageType = 'T'
)

func InitializeHandlers() map[byte]ResponseHandler {
	handlers := make(map[byte]ResponseHandler)

	return handlers
}

func ProcessReadyForQuery(reader *PgReader) (string, error) {
	status, err := reader.ReadByte()
	if err != nil {
		return "", fmt.Errorf("error reading ready for query status: %w", err)
	}
	return string(status), nil
}

func ProcessBackendKeyData(reader *PgReader) (int32, int32, error) {
	pid, err := reader.ReadInt32()
	if err != nil {
		return 0, 0, fmt.Errorf("error reading backend key data: %w", err)
	}
	key, err := reader.ReadInt32()
	if err != nil {
		return 0, 0, fmt.Errorf("error reading process ID: %w", err)
	}

	return pid, key, nil
}

func ProcessParameterStatus(reader *PgReader) (string, string, error) {
	param, err := reader.ReadCString()
	if err != nil {
		return "", "", fmt.Errorf("error reading parameter name: %w", err)
	}

	value, err := reader.ReadCString()
	if err != nil {
		return "", "", fmt.Errorf("error reading parameter value: %w", err)
	}

	return param, value, nil
}

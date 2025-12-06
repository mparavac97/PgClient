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
	Query                MessageType = 'Q'
	Parse                MessageType = 'P'
)

func InitializeHandlers() map[byte]ResponseHandler {
	handlers := make(map[byte]ResponseHandler)

	return handlers
}

func ProcessErrorResponse(reader *PgReader, length int32) (map[byte]string, error) {
    fields := make(map[byte]string)
    bytesRemaining := length - 4 // length includes itself

    for bytesRemaining > 0 {
        fieldType, err := reader.ReadByte()
        if err != nil {
            return nil, fmt.Errorf("error reading error field type: %w", err)
        }

        // Terminator
        if fieldType == 0 {
            break
        }

        str, err := reader.ReadCString()
        if err != nil {
            return nil, fmt.Errorf("error reading error field string: %w", err)
        }

        fields[fieldType] = str
        bytesRemaining -= int32(1 + len(str) + 1)
    }

    return fields, nil
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

func (mt MessageType) String() string {
	switch mt {
	case AuthenticationOK:
		return "AuthenticationOK"
	case BackendKeyData:
		return "BackendKeyData"
	case BindComplete:
		return "BindComplete"
	case CloseComplete:
		return "CloseComplete"
	case CommandComplete:
		return "CommandComplete"
	case DataRow:
		return "DataRow"
	case EmptyQueryResponse:
		return "EmptyQueryResponse"
	case ErrorResponse:
		return "ErrorResponse"
	case FunctionCallResponse:
		return "FunctionCallResponse"
	case NoData:
		return "NoData"
	case NoticeResponse:
		return "NoticeResponse"
	case NotificationResponse:
		return "NotificationResponse"
	case ParameterDescription:
		return "ParameterDescription"
	case ParameterStatus:
		return "ParameterStatus"
	case ParseComplete:
		return "ParseComplete"
	case PortalSuspended:
		return "PortalSuspended"
	case ReadyForQuery:
		return "ReadyForQuery"
	case RowDescription:
		return "RowDescription"
	case Query:
		return "Query"
	case Parse:
		return "Parse"
	default:
		return fmt.Sprintf("Unknown(%c)", mt)
	}
}

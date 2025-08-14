package message

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
	handlers[byte(ParameterStatus)] = ProcessParameterStatus

	return handlers
}

func ProcessMessageType(msgType MessageType) {

}

func ProcessParameterStatus(msgType MessageType, data []byte) (any, error) {
	params := make(map[string]string)

	return params, nil
}

package client

type PgCommand struct {
	connection  *PgConnection
	commandText string
	params      map[string]any
	paramNames  []string
}

func NewPgCommand(commandText string, conn *PgConnection) *PgCommand {
	return &PgCommand{
		connection:  conn,
		commandText: commandText,
		params:      make(map[string]any),
		paramNames:  make([]string, 0),
	}
}

func (cmd *PgCommand) SetParameter(name string, value any) {
	if cmd.params == nil {
		cmd.params = make(map[string]any)
	}
	cmd.params[name] = value
	cmd.paramNames = append(cmd.paramNames, name)
}

func (cmd *PgCommand) Execute() (*QueryResult, error) {
	// Create a buffered channel for the query result
	resultChan := make(chan QueryResult, 1)
	// Queue the query
	cmd.connection.queryQueue <- QueryRequest{
		query:      cmd.commandText,
		result:     resultChan,
		params:     cmd.params,
		paramNames: cmd.paramNames,
	}

	// Wait for and process the result
	result := <-resultChan
	return &result, result.err
}

package client

import "fmt"

type PgCommand struct {
	connection  *PgConnection
	commandText string
	params      map[string]any
}

func NewPgCommand(commandText string, conn *PgConnection) *PgCommand {
	return &PgCommand{
		connection:  conn,
		commandText: commandText,
		params:      make(map[string]any),
	}
}

func (cmd *PgCommand) SetParameter(name string, value any) {
	if cmd.params == nil {
		cmd.params = make(map[string]any)
	}
	cmd.params[name] = value
}

func (cmd *PgCommand) Execute() (*QueryResult, error) {
	// Create a buffered channel for the query result
	resultChan := make(chan QueryResult, 1)
	fmt.Println("[Execute] Command to execute: ", cmd.commandText)
	// Queue the query
	cmd.connection.queryQueue <- QueryRequest{
		query:  cmd.commandText,
		result: resultChan,
		params: cmd.params,
	}

	// Wait for and process the result
	result := <-resultChan
	return &result, result.err
}

package client

import (
	"context"
	"net"
	"time"
)

type TCPClient struct {
	conn net.Conn
	host string
	port string
}

func NewTCPClient(host, port string) *TCPClient {
	return &TCPClient{
		host: host,
		port: port,
	}
}

func (client *TCPClient) ConnectToServer(ctx context.Context) error {
	// Get timeout from context if set
	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	// Create dialer with or without timeout
	dialer := net.Dialer{}
	if timeout > 0 {
		dialer.Timeout = timeout
	}

	// Use context for overall operation timeout
	conn, err := dialer.DialContext(ctx, "tcp", client.host+":"+client.port)
	if err != nil {
		return err
	}

	client.conn = conn

	// Set read/write deadlines only if timeout is specified
	if deadline, ok := ctx.Deadline(); ok {
		client.conn.SetDeadline(deadline)
	}

	return nil
}

func (client *TCPClient) Close() error {
	if client.conn != nil {
		return client.conn.Close()
	}
	return nil
}

package main

import (
	"net"
)

type TCPClient struct {
	conn net.Conn
	host string
	port string
	//timeout time.Duration
}

func NewTCPClient(host, port string) *TCPClient {
	return &TCPClient{
		host: host,
		port: port,
	}
}

func (client *TCPClient) ConnectToServer() error {
	conn, err := net.Dial("tcp", client.host+":"+client.port)
	if err != nil {
		return err
	}
	client.conn = conn
	//defer client.conn.Close()
	return nil
}

func (client *TCPClient) Close() error {
	if client.conn != nil {
		return client.conn.Close()
	}
	return nil
}

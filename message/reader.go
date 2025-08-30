package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type PgReader struct {
	reader io.Reader
}

func NewPgReader(reader io.Reader) *PgReader {
	return &PgReader{reader: reader}
}

func (r *PgReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if err != nil {
		return n, err
	}
	return n, nil
}

func (r *PgReader) ReadByte() (byte, error) {
	var buf [1]byte
	_, err := r.reader.Read(buf[:])
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (r *PgReader) ReadInt32() (int32, error) {
	var buf [4]byte
	n, err := r.reader.Read(buf[:])
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(buf[:n])), nil
}

func (r *PgReader) ReadInt16() (int16, error) {
	var buf [2]byte
	n, err := r.reader.Read(buf[:])
	if err != nil {
		return 0, err
	}
	// return int16(buf[0])<<8 | int16(buf[1]), nil - this is a more efficient way to read int16
	return int16(binary.BigEndian.Uint16(buf[:n])), nil
}

func (r *PgReader) ReadCString() (string, error) {
	var buf bytes.Buffer
	one := make([]byte, 1)

	for {
		_, err := r.reader.Read(one)
		if err != nil {
			return "", err
		}
		if one[0] == 0 {
			break
		}
		buf.WriteByte(one[0])
	}
	return buf.String(), nil
}

func (conn *PgReader) ReadNBytes(n int) []byte {
	b := make([]byte, n)
	io.ReadFull(conn, b[:])
	return b
}

func (reader *PgReader) SkipN(n int32) error {
	_, err := io.CopyN(io.Discard, reader, int64(n))
	if err != nil && err != io.EOF {
		return fmt.Errorf("error skiSpping %d bytes: %w", n, err)
	}
	return nil
}

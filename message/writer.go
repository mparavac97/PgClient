package message

import "io"

type PgWriter struct {
	writer io.Writer
}

func NewPgWriter(writer io.Writer) *PgWriter {
	return &PgWriter{writer: writer}
}

func (w *PgWriter) Write(p []byte) (n int, err error) {
	n, err = w.writer.Write(p)
	if err != nil {
		return n, err
	}
	return n, nil
}

package syslog

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type frameReader interface {
	Next() ([]byte, error)
}

func newFrameReader(r *bufio.Reader, framing, trailer string, maxBytes int) (frameReader, error) {
	switch strings.ToLower(framing) {
	case "", "auto":
		b, err := r.Peek(1)
		if err != nil {
			return nil, err
		}
		if b[0] >= '0' && b[0] <= '9' {
			return &octetFrameReader{r: r, maxBytes: maxBytes}, nil
		}
		return &transparentFrameReader{r: r, trailer: normalizeTrailer(trailer), maxBytes: maxBytes}, nil
	case "octet-counting":
		return &octetFrameReader{r: r, maxBytes: maxBytes}, nil
	case "non-transparent":
		return &transparentFrameReader{r: r, trailer: normalizeTrailer(trailer), maxBytes: maxBytes}, nil
	default:
		return nil, fmt.Errorf("unknown syslog framing %q", framing)
	}
}

type octetFrameReader struct {
	r        *bufio.Reader
	maxBytes int
}

func (f *octetFrameReader) Next() ([]byte, error) {
	lenText, err := f.r.ReadString(' ')
	if err != nil {
		return nil, err
	}
	lenText = strings.TrimSuffix(lenText, " ")
	if lenText == "" {
		return nil, fmt.Errorf("empty octet-counting length")
	}
	n, err := strconv.Atoi(lenText)
	if err != nil || n < 0 {
		return nil, fmt.Errorf("invalid octet-counting length")
	}
	if f.maxBytes > 0 && n > f.maxBytes {
		if _, copyErr := io.CopyN(io.Discard, f.r, int64(n)); copyErr != nil {
			return nil, copyErr
		}
		return nil, errFrameTooLarge
	}
	msg := make([]byte, n)
	if _, err := io.ReadFull(f.r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

type transparentFrameReader struct {
	r        *bufio.Reader
	trailer  int
	maxBytes int
}

func (f *transparentFrameReader) Next() ([]byte, error) {
	if f.trailer >= 0 {
		return f.readUntil(byte(f.trailer))
	}

	var msg []byte
	for {
		b, err := f.r.ReadByte()
		if err != nil {
			if err == io.EOF && len(msg) > 0 {
				return trimCR(msg), nil
			}
			return nil, err
		}
		if b == '\n' || b == 0 {
			f.trailer = int(b)
			return trimCR(msg), nil
		}
		msg = append(msg, b)
		if f.maxBytes > 0 && len(msg) > f.maxBytes {
			return nil, errFrameTooLarge
		}
	}
}

func (f *transparentFrameReader) readUntil(delim byte) ([]byte, error) {
	msg, err := f.r.ReadBytes(delim)
	if err != nil {
		if err == io.EOF && len(msg) > 0 {
			return trimCR(msg), nil
		}
		return nil, err
	}
	msg = msg[:len(msg)-1]
	if f.maxBytes > 0 && len(msg) > f.maxBytes {
		return nil, errFrameTooLarge
	}
	return trimCR(msg), nil
}

func normalizeTrailer(trailer string) int {
	switch strings.ToLower(trailer) {
	case "lf", "crlf":
		return '\n'
	case "nul":
		return 0
	default:
		return -1
	}
}

func trimCR(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\r' {
		return b[:len(b)-1]
	}
	return b
}

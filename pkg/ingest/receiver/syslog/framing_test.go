package syslog

import (
	"bufio"
	"errors"
	"strings"
	"testing"
)

func TestOctetCountingFraming(t *testing.T) {
	fr, err := newFrameReader(bufio.NewReader(strings.NewReader("5 hello6 world!")), "auto", "auto", 64)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := fr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "hello" {
		t.Fatalf("first = %q", msg)
	}
	msg, err = fr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "world!" {
		t.Fatalf("second = %q", msg)
	}
}

func TestNonTransparentFraming(t *testing.T) {
	fr, err := newFrameReader(bufio.NewReader(strings.NewReader("one\r\ntwo\r\n")), "auto", "auto", 64)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := fr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "one" {
		t.Fatalf("first = %q", msg)
	}
	msg, err = fr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "two" {
		t.Fatalf("second = %q", msg)
	}
}

func TestFrameTooLarge(t *testing.T) {
	fr, err := newFrameReader(bufio.NewReader(strings.NewReader("6 hello!")), "octet-counting", "auto", 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fr.Next(); !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("err = %v", err)
	}
}

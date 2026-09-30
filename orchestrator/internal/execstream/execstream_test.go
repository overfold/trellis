package execstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/api"
)

func TestFramesRoundTripAndSplitData(t *testing.T) {
	var buffer bytes.Buffer
	writer := NewWriter(&buffer, 0)
	large := bytes.Repeat([]byte("x"), MaxPayload+10)
	if err := writer.WriteData(FrameStdout, large); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteJSON(FrameExit, api.ExecExit{ExitCode: 3}); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteData(FrameStdin, nil); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(&buffer)
	var output []byte
	for _, want := range []int{MaxPayload, 10} {
		frame, err := reader.Next()
		if err != nil || frame.Type != FrameStdout || len(frame.Payload) != want {
			t.Fatalf("frame = %d with %d bytes, %v; want stdout with %d", frame.Type, len(frame.Payload), err, want)
		}
		output = append(output, frame.Payload...)
	}
	if !bytes.Equal(output, large) {
		t.Fatal("split output does not reassemble")
	}
	frame, err := reader.Next()
	if err != nil || frame.Type != FrameExit || string(frame.Payload) != `{"exit_code":3}` {
		t.Fatalf("exit frame = %d %q, %v", frame.Type, frame.Payload, err)
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("empty data wrote a frame; next error = %v", err)
	}
}

func TestReaderRejectsMalformedFrames(t *testing.T) {
	frame := func(frameType byte, length uint32, payload string) []byte {
		data := []byte{frameType, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(data[1:], length)
		return append(data, payload...)
	}
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "unknown type", data: frame(9, 0, ""), want: ErrInvalidFrame},
		{name: "zero type", data: frame(0, 0, ""), want: ErrInvalidFrame},
		{name: "oversized payload", data: frame(byte(FrameStdin), MaxPayload+1, ""), want: ErrInvalidFrame},
		{name: "truncated payload", data: frame(byte(FrameStdin), 4, "ab"), want: io.ErrUnexpectedEOF},
		{name: "truncated header", data: []byte{byte(FrameStdin), 0}, want: io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewReader(bytes.NewReader(tt.data)).Next(); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
	if err := NewWriter(io.Discard, 0).WriteFrame(FrameResize, make([]byte, MaxPayload+1)); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("oversized write error = %v", err)
	}
}

func TestFrameDirections(t *testing.T) {
	for _, frameType := range []FrameType{FrameStdin, FrameStdinClose, FrameResize} {
		if !ClientFrame(frameType) || ServerFrame(frameType) {
			t.Fatalf("frame %d is not client-only", frameType)
		}
	}
	for _, frameType := range []FrameType{FrameStdout, FrameStderr, FrameExit, FrameError} {
		if ClientFrame(frameType) || !ServerFrame(frameType) {
			t.Fatalf("frame %d is not server-only", frameType)
		}
	}
}

func TestRequestRoundTripAndDefaults(t *testing.T) {
	request := api.ExecRequest{Task: "web", Command: []string{"sh", "-c", "echo a&b=c"}, TTY: true, Stdin: true, Term: "xterm-256color", Cols: 120, Rows: 40}
	decoded, err := DecodeRequest(EncodeRequest(request))
	if err != nil || !reflect.DeepEqual(decoded, request) {
		t.Fatalf("decoded = %#v, %v", decoded, err)
	}
	decoded, err = DecodeRequest(url.Values{"command": {"sh"}, "tty": {"true"}})
	if err != nil || decoded.Cols != DefaultCols || decoded.Rows != DefaultRows {
		t.Fatalf("defaulted = %#v, %v", decoded, err)
	}
	agent, err := DecodeAgentRequest(EncodeAgentRequest(api.AgentExecRequest{ExecRequest: request, Epoch: 9}))
	if err != nil || agent.Epoch != 9 || !reflect.DeepEqual(agent.ExecRequest, request) {
		t.Fatalf("agent request = %#v, %v", agent, err)
	}
}

func TestDecodeRequestRejectsInvalidValues(t *testing.T) {
	for _, query := range []string{
		"",
		"command=",
		"command=sh&tty=maybe",
		"command=sh&stdin=2",
		"command=sh&cols=80",
		"command=sh&term=xterm",
		"command=sh&tty=true&cols=0",
		"command=sh&tty=true&rows=1001",
		"command=sh&tty=true&term=xterm%0A",
		"command=sh&tty=true&term=" + strings.Repeat("x", 65),
		"command=sh&" + strings.Repeat("command=x&", maxCommandArgs),
	} {
		values, err := url.ParseQuery(query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeRequest(values); err == nil {
			t.Fatalf("query %q was accepted", query)
		}
	}
	for _, query := range []string{"command=sh", "command=sh&epoch=0", "command=sh&epoch=x"} {
		values, _ := url.ParseQuery(query)
		if _, err := DecodeAgentRequest(values); err == nil {
			t.Fatalf("agent query %q was accepted", query)
		}
	}
	if err := ValidateResize(api.ExecResize{Cols: 0, Rows: 1}); err == nil {
		t.Fatal("zero columns accepted")
	}
	if err := ValidateResize(api.ExecResize{Cols: 1000, Rows: 1000}); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptUpgradesAndCarriesFramesBothWays(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !IsUpgradeRequest(r) {
			http.Error(w, "upgrade required", http.StatusBadRequest)
			return
		}
		conn, err := Accept(w)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		frame, err := NewReader(conn).Next()
		if err != nil {
			t.Errorf("read: %v", err)
			return
		}
		_ = NewWriter(conn, 0).WriteData(FrameStdout, frame.Payload)
	}))
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = plain.Body.Close()
	if _, err := Upgraded(plain); err == nil || plain.StatusCode != http.StatusBadRequest {
		t.Fatalf("plain request status = %d, upgrade error = %v", plain.StatusCode, err)
	}

	SetUpgradeHeaders(request)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := Upgraded(response)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if err := NewWriter(stream, 0).WriteData(FrameStdin, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	frame, err := NewReader(stream).Next()
	if err != nil || frame.Type != FrameStdout || string(frame.Payload) != "ping" {
		t.Fatalf("frame = %d %q, %v", frame.Type, frame.Payload, err)
	}
}

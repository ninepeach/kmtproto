package kmtproto

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONCodecRoundTrip(t *testing.T) {
	codec := NewJSONCodec()
	want := Envelope{V: WireVersionV2, Type: FrameSend, ID: "msg_1", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{"text":"hello"}`)})}
	encoded, err := codec.Encode(&want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.ID != want.ID || got.SessionID != want.SessionID {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}

func TestJSONCodecRejectsUnknownEnvelopeField(t *testing.T) {
	codec := NewJSONCodec()
	_, err := codec.Decode([]byte(`{"v":2,"type":"HELLO","payload":{},"surprise":true}`))
	if err == nil {
		t.Fatal("expected strict decoding error")
	}
}

func TestJSONCodecLimitsInputBeforeDecode(t *testing.T) {
	codec := NewJSONCodec()
	codec.Limits.MaxFrameSize = 16
	_, err := codec.Decode([]byte(strings.Repeat("x", 17)))
	if err == nil {
		t.Fatal("expected frame size error")
	}
}

func FuzzJSONCodec(f *testing.F) {
	f.Add([]byte(`{"v":2,"type":"HELLO","payload":{}}`))
	f.Add([]byte(`not-json`))
	f.Fuzz(func(t *testing.T, data []byte) {
		codec := NewJSONCodec()
		codec.Limits.MaxFrameSize = 64 << 10
		_, _ = codec.Decode(data)
	})
}

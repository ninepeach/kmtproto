package kmtproto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

type JSONCodec struct {
	Limits Limits
	Strict bool
}

func NewJSONCodec() *JSONCodec {
	return &JSONCodec{Limits: DefaultLimits(), Strict: true}
}

func (c *JSONCodec) Encode(frame *Envelope) ([]byte, error) {
	if frame == nil {
		return nil, NewProtocolError(ErrorBadRequest, "nil frame")
	}
	if err := ValidateFrame(frame, c.limits(), c.Strict); err != nil {
		return nil, err
	}
	b, err := json.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("encode frame: %w", err)
	}
	if len(b) > c.limits().MaxFrameSize {
		return nil, NewProtocolError(ErrorBadRequest, "frame exceeds maximum size")
	}
	return b, nil
}

func (c *JSONCodec) Decode(data []byte) (*Envelope, error) {
	if len(data) == 0 {
		return nil, NewProtocolError(ErrorBadRequest, "empty frame")
	}
	if len(data) > c.limits().MaxFrameSize {
		return nil, NewProtocolError(ErrorBadRequest, "frame exceeds maximum size")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if c.Strict {
		dec.DisallowUnknownFields()
	}
	var frame Envelope
	if err := dec.Decode(&frame); err != nil {
		return nil, NewProtocolError(ErrorBadRequest, "invalid envelope JSON: "+err.Error())
	}
	if err := ensureJSONEOF(dec); err != nil {
		return nil, NewProtocolError(ErrorBadRequest, err.Error())
	}
	if err := ValidateFrame(&frame, c.limits(), c.Strict); err != nil {
		return nil, err
	}
	return &frame, nil
}

func (c *JSONCodec) limits() Limits {
	if c.Limits.MaxFrameSize == 0 {
		return DefaultLimits()
	}
	return c.Limits
}

func decodePayload(raw json.RawMessage, dst any, strict bool) error {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return NewProtocolError(ErrorBadRequest, "missing payload")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if strict {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(dst); err != nil {
		return NewProtocolError(ErrorBadRequest, "invalid payload: "+err.Error())
	}
	if err := ensureJSONEOF(dec); err != nil {
		return NewProtocolError(ErrorBadRequest, err.Error())
	}
	return nil
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return fmt.Errorf("invalid trailing JSON: %w", err)
	}
	return nil
}

func mustPayload(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

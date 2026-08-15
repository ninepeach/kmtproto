package kmtproto

type Codec interface {
	Encode(frame *Envelope) ([]byte, error)
	Decode(data []byte) (*Envelope, error)
}

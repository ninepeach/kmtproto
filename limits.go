package kmtproto

const (
	DefaultMaxReplayEvents     uint64 = 4096
	DefaultMaxReplayBytes             = 16 << 20
	DefaultEventIdentityWindow        = 4096
)

type Limits struct {
	MaxFrameSize          int
	MaxPayloadSize        int
	MaxIDLength           int
	MaxSessionIDLength    int
	MaxErrorMessageLength int
}

func DefaultLimits() Limits {
	return Limits{
		MaxFrameSize:          1 << 20,
		MaxPayloadSize:        768 << 10,
		MaxIDLength:           128,
		MaxSessionIDLength:    128,
		MaxErrorMessageLength: 1024,
	}
}

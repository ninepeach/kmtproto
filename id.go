package kmtproto

import (
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
)

const crockfordBase32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewULID creates a lexicographically sortable 128-bit ULID without adding an
// external dependency. Callers can inject both Clock and randomness in tests.
func NewULID(clock Clock, random io.Reader) (string, error) {
	if clock == nil {
		clock = RealClock{}
	}
	if random == nil {
		random = rand.Reader
	}
	var raw [16]byte
	millis := uint64(clock.Now().UnixMilli())
	if millis > (1<<48)-1 {
		return "", fmt.Errorf("kmtproto: ULID timestamp out of range")
	}
	for i := 5; i >= 0; i-- {
		raw[i] = byte(millis)
		millis >>= 8
	}
	if _, err := io.ReadFull(random, raw[6:]); err != nil {
		return "", fmt.Errorf("kmtproto: generate ULID randomness: %w", err)
	}
	n := new(big.Int).SetBytes(raw[:])
	base := big.NewInt(32)
	rem := new(big.Int)
	out := make([]byte, 26)
	for i := len(out) - 1; i >= 0; i-- {
		n.QuoRem(n, base, rem)
		out[i] = crockfordBase32[rem.Int64()]
	}
	return string(out), nil
}

func DefaultSessionIDGenerator(clock Clock) func() (string, error) {
	return func() (string, error) {
		id, err := NewULID(clock, rand.Reader)
		if err != nil {
			return "", err
		}
		return "s_" + id, nil
	}
}

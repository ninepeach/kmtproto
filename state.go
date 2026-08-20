package kmtproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// CapabilityStateSync gates every v0.2 State Synchronization wire frame.
	CapabilityStateSync = "state-sync"
	// CapabilityStateSyncVersion is the exact State Frame semantics implemented
	// by this wire version. Capability versions are not implicitly compatible.
	CapabilityStateSyncVersion uint16 = 1
)

func stateSyncEnabled(capabilities SessionCapabilities) bool {
	version, enabled := capabilities.Version(CapabilityStateSync)
	return enabled && version == CapabilityStateSyncVersion
}

var (
	ErrStateStale            = errors.New("kmtproto: stale state version")
	ErrStateConflict         = errors.New("kmtproto: state version conflict")
	ErrStateIdentityMismatch = errors.New("kmtproto: state identity mismatch")
)

// StateObject is one complete protocol-level State replacement. State version
// is scoped only to (namespace, object_id) and is independent from EVENT seq.
type StateObject struct {
	Namespace string          `json:"namespace"`
	ObjectID  string          `json:"object_id"`
	Version   uint64          `json:"version"`
	Data      json.RawMessage `json:"data"`
}

// StateIdentity uniquely identifies a State Object independently of Session,
// transport connection, and EVENT sequence.
type StateIdentity struct {
	Namespace string
	ObjectID  string
}

// Identity returns the object-scoped State identity.
func (o StateObject) Identity() StateIdentity {
	return StateIdentity{Namespace: o.Namespace, ObjectID: o.ObjectID}
}

// NewStateObject validates and defensively copies a complete State Object.
func NewStateObject(namespace, objectID string, version uint64, data json.RawMessage, limits Limits) (StateObject, error) {
	object := StateObject{
		Namespace: namespace,
		ObjectID:  objectID,
		Version:   version,
		Data:      append(json.RawMessage(nil), data...),
	}
	if err := ValidateStateObject(&object, limits); err != nil {
		return StateObject{}, err
	}
	return object, nil
}

// ValidateStateIdentity validates the application-opaque identity fields used
// at the protocol boundary.
func ValidateStateIdentity(namespace, objectID string, limits Limits) error {
	limits = normalizeLimits(limits)
	if err := validateStateNamespace(namespace, limits.MaxStateNamespaceLength); err != nil {
		return err
	}
	if objectID == "" || len(objectID) > limits.MaxStateObjectIDLength || !utf8.ValidString(objectID) {
		return NewProtocolError(ErrorBadRequest, "invalid or oversized state object_id")
	}
	for _, r := range objectID {
		if unicode.IsControl(r) {
			return NewProtocolError(ErrorBadRequest, "state object_id must not contain control characters")
		}
	}
	return nil
}

// ValidateStateObject validates identity, positive non-exhausted version, JSON
// data, and configured State/payload size bounds.
func ValidateStateObject(object *StateObject, limits Limits) error {
	limits = normalizeLimits(limits)
	if object == nil {
		return NewProtocolError(ErrorBadRequest, "nil state object")
	}
	if err := ValidateStateIdentity(object.Namespace, object.ObjectID, limits); err != nil {
		return err
	}
	if object.Version == 0 || object.Version == math.MaxUint64 {
		return NewProtocolError(ErrorInvalidStateVersion, "state version must be positive and non-exhausted")
	}
	if len(object.Data) == 0 || !json.Valid(object.Data) {
		return NewProtocolError(ErrorBadRequest, "state data must contain one valid JSON value")
	}
	if len(object.Data) > limits.MaxStateDataSize {
		return NewProtocolError(ErrorBadRequest, "state data exceeds maximum size")
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return NewProtocolError(ErrorBadRequest, "state object cannot be encoded: "+err.Error())
	}
	if len(encoded) > limits.MaxStateObjectSize || len(encoded) > limits.MaxPayloadSize {
		return NewProtocolError(ErrorBadRequest, "state object exceeds maximum size")
	}
	return nil
}

func validateStateNamespace(namespace string, maxLength int) error {
	if namespace == "" || len(namespace) > maxLength || !utf8.ValidString(namespace) {
		return NewProtocolError(ErrorBadRequest, "invalid or oversized state namespace")
	}
	if namespace[0] < 'a' || namespace[0] > 'z' {
		return NewProtocolError(ErrorBadRequest, "invalid state namespace: "+namespace)
	}
	separator := false
	for i := 1; i < len(namespace); i++ {
		ch := namespace[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9':
			separator = false
		case ch == '.' || ch == '-':
			if separator {
				return NewProtocolError(ErrorBadRequest, "invalid state namespace: "+namespace)
			}
			separator = true
		default:
			return NewProtocolError(ErrorBadRequest, "invalid state namespace: "+namespace)
		}
	}
	if separator {
		return NewProtocolError(ErrorBadRequest, "invalid state namespace: "+namespace)
	}
	return nil
}

// StateApplyResult describes the deterministic result of comparing one
// incoming complete State replacement with the current object.
type StateApplyResult uint8

const (
	StateApplyApplied StateApplyResult = iota + 1
	StateApplyDuplicate
	StateApplyStale
	StateApplyConflict
)

func (r StateApplyResult) String() string {
	switch r {
	case StateApplyApplied:
		return "APPLIED"
	case StateApplyDuplicate:
		return "DUPLICATE"
	case StateApplyStale:
		return "STALE"
	case StateApplyConflict:
		return "CONFLICT"
	default:
		return "UNKNOWN"
	}
}

// ApplyStateObject is a pure monotonic merge. It never mutates current or
// incoming. Newer versions replace, older versions return ErrStateStale, equal
// equivalent values are duplicates, and equal differing values return
// ErrStateConflict. State versions may jump and never trigger EVENT gaps.
func ApplyStateObject(current *StateObject, incoming StateObject, limits Limits) (StateObject, StateApplyResult, error) {
	if err := ValidateStateObject(&incoming, limits); err != nil {
		return StateObject{}, 0, err
	}
	if current == nil {
		return cloneStateObject(incoming), StateApplyApplied, nil
	}
	if err := ValidateStateObject(current, limits); err != nil {
		return StateObject{}, 0, err
	}
	retained := cloneStateObject(*current)
	if current.Identity() != incoming.Identity() {
		return retained, StateApplyConflict, ErrStateIdentityMismatch
	}
	switch {
	case incoming.Version > current.Version:
		return cloneStateObject(incoming), StateApplyApplied, nil
	case incoming.Version < current.Version:
		return retained, StateApplyStale, ErrStateStale
	}
	equal, err := equalStateData(current.Data, incoming.Data)
	if err != nil {
		return retained, StateApplyConflict, err
	}
	if equal {
		return retained, StateApplyDuplicate, nil
	}
	return retained, StateApplyConflict, ErrStateConflict
}

func cloneStateObject(object StateObject) StateObject {
	object.Data = append(json.RawMessage(nil), object.Data...)
	return object
}

type stateSnapshotAccumulator struct {
	protocolLimits Limits
	snapshotLimits StateSnapshotLimits
	states         []StateObject
	encodedBytes   int
}

func newStateSnapshotAccumulator(protocolLimits Limits, snapshotLimits StateSnapshotLimits) (*stateSnapshotAccumulator, error) {
	protocolLimits = normalizeLimits(protocolLimits)
	if snapshotLimits.MaxObjects <= 0 || snapshotLimits.MaxBytes <= 0 {
		return nil, ErrStateSnapshotLimitExceeded
	}
	baseBytes := len(`{"states":[]}`)
	if baseBytes > snapshotLimits.MaxBytes {
		return nil, ErrStateSnapshotLimitExceeded
	}
	return &stateSnapshotAccumulator{
		protocolLimits: protocolLimits,
		snapshotLimits: snapshotLimits,
		states:         make([]StateObject, 0, minInt(snapshotLimits.MaxObjects, 16)),
		encodedBytes:   baseBytes,
	}, nil
}

func (a *stateSnapshotAccumulator) Add(object StateObject) error {
	if len(a.states) >= a.snapshotLimits.MaxObjects {
		return ErrStateSnapshotLimitExceeded
	}
	if err := ValidateStateObject(&object, a.protocolLimits); err != nil {
		return err
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return NewProtocolError(ErrorBadRequest, "state object cannot be encoded: "+err.Error())
	}
	additional := len(encoded)
	if len(a.states) > 0 {
		additional++
	}
	if additional > a.snapshotLimits.MaxBytes-a.encodedBytes {
		return ErrStateSnapshotLimitExceeded
	}
	a.encodedBytes += additional
	a.states = append(a.states, cloneStateObject(object))
	return nil
}

func (a *stateSnapshotAccumulator) Payload() (json.RawMessage, error) {
	payload, err := json.Marshal(StateSnapshotPayload{States: a.states})
	if err != nil {
		return nil, NewProtocolError(ErrorBadRequest, "State snapshot cannot be encoded: "+err.Error())
	}
	if len(payload) > a.snapshotLimits.MaxBytes {
		return nil, ErrStateSnapshotLimitExceeded
	}
	return payload, nil
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func equalStateData(left, right json.RawMessage) (bool, error) {
	leftValue, err := decodeStateData(left)
	if err != nil {
		return false, err
	}
	rightValue, err := decodeStateData(right)
	if err != nil {
		return false, err
	}
	return equalStateJSONValue(leftValue, rightValue), nil
}

func decodeStateData(data json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, NewProtocolError(ErrorBadRequest, "invalid state data: "+err.Error())
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, NewProtocolError(ErrorBadRequest, "invalid state data: "+err.Error())
	}
	return value, nil
}

func equalStateJSONValue(left, right any) bool {
	switch leftValue := left.(type) {
	case nil:
		return right == nil
	case bool:
		rightValue, ok := right.(bool)
		return ok && leftValue == rightValue
	case string:
		rightValue, ok := right.(string)
		return ok && leftValue == rightValue
	case json.Number:
		rightValue, ok := right.(json.Number)
		if !ok {
			return false
		}
		leftNumber, leftOK := normalizeStateJSONNumber(leftValue)
		rightNumber, rightOK := normalizeStateJSONNumber(rightValue)
		return leftOK && rightOK && leftNumber == rightNumber
	case []any:
		rightValue, ok := right.([]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for i := range leftValue {
			if !equalStateJSONValue(leftValue[i], rightValue[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		rightValue, ok := right.(map[string]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, value := range leftValue {
			rightItem, exists := rightValue[key]
			if !exists || !equalStateJSONValue(value, rightItem) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

type normalizedStateJSONNumber struct {
	negative    bool
	coefficient string
	exponent    string
}

func normalizeStateJSONNumber(number json.Number) (normalizedStateJSONNumber, bool) {
	text := number.String()
	negative := strings.HasPrefix(text, "-")
	if negative {
		text = text[1:]
	}
	exponentText := "0"
	if index := strings.IndexAny(text, "eE"); index >= 0 {
		exponentText = text[index+1:]
		text = text[:index]
	}
	exponent := new(big.Int)
	if _, ok := exponent.SetString(exponentText, 10); !ok {
		return normalizedStateJSONNumber{}, false
	}
	fractionDigits := 0
	if index := strings.IndexByte(text, '.'); index >= 0 {
		fractionDigits = len(text) - index - 1
		text = text[:index] + text[index+1:]
	}
	digits := strings.TrimLeft(text, "0")
	if digits == "" {
		return normalizedStateJSONNumber{coefficient: "0", exponent: "0"}, true
	}
	trailingZeros := len(digits) - len(strings.TrimRight(digits, "0"))
	if trailingZeros > 0 {
		digits = digits[:len(digits)-trailingZeros]
	}
	exponent.Sub(exponent, big.NewInt(int64(fractionDigits)))
	exponent.Add(exponent, big.NewInt(int64(trailingZeros)))
	return normalizedStateJSONNumber{negative: negative, coefficient: digits, exponent: exponent.String()}, true
}

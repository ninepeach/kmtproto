package kmtproto

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// CapabilityOffer describes one capability and the versions a client can use.
// Required capabilities fail negotiation when no mutually supported version
// exists. The Versions slice is copied by protocol constructors.
type CapabilityOffer struct {
	Name     string   `json:"name"`
	Versions []uint16 `json:"versions"`
	Required bool     `json:"required,omitempty"`
}

// CapabilitySpec describes the versions of one capability supported by a
// server. It is configuration, not an implementation of the capability.
type CapabilitySpec struct {
	Name     string
	Versions []uint16
}

// NegotiatedCapability is the immutable result for one accepted capability.
type NegotiatedCapability struct {
	Name    string `json:"name"`
	Version uint16 `json:"version"`
}

// SessionCapabilities is an immutable, concurrency-safe snapshot of the
// capabilities enabled for one protocol Session. Its zero value represents a
// Session with no negotiated capabilities.
type SessionCapabilities struct {
	selections []NegotiatedCapability
	versions   map[string]uint16
}

// NewSessionCapabilities validates and defensively copies a canonical
// negotiated capability list into immutable Session protocol state.
func NewSessionCapabilities(capabilities []NegotiatedCapability, limits Limits) (SessionCapabilities, error) {
	if err := ValidateNegotiatedCapabilities(capabilities, limits); err != nil {
		return SessionCapabilities{}, err
	}
	result := SessionCapabilities{
		selections: cloneNegotiatedCapabilities(capabilities),
		versions:   make(map[string]uint16, len(capabilities)),
	}
	for _, capability := range capabilities {
		result.versions[capability.Name] = capability.Version
	}
	return result, nil
}

// Enabled reports whether a capability was negotiated for the Session.
func (c SessionCapabilities) Enabled(name string) bool {
	_, enabled := c.versions[name]
	return enabled
}

// Version returns the negotiated capability version when the capability is
// enabled for the Session.
func (c SessionCapabilities) Version(name string) (uint16, bool) {
	version, enabled := c.versions[name]
	return version, enabled
}

// List returns the canonical negotiated capability list as a defensive copy.
func (c SessionCapabilities) List() []NegotiatedCapability {
	return cloneNegotiatedCapabilities(c.selections)
}

// CapabilityRegistry is an immutable, concurrency-safe registry of capability
// names and versions. Registration does not add frame or business behavior.
type CapabilityRegistry struct {
	versions map[string][]uint16
}

// NewCapabilityRegistry validates and copies server capability specifications.
// Capability names must be unique. Version order in the input is irrelevant.
func NewCapabilityRegistry(specs []CapabilitySpec, limits Limits) (*CapabilityRegistry, error) {
	limits = normalizeLimits(limits)
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	if len(specs) > limits.MaxCapabilities {
		return nil, NewProtocolError(ErrorInvalidCapability, "too many capability specifications")
	}
	registry := &CapabilityRegistry{versions: make(map[string][]uint16, len(specs))}
	for _, spec := range specs {
		if err := validateCapabilityName(spec.Name, limits); err != nil {
			return nil, err
		}
		if _, exists := registry.versions[spec.Name]; exists {
			return nil, NewProtocolError(ErrorInvalidCapability, "duplicate capability specification: "+spec.Name)
		}
		versions, err := validateAndSortCapabilityVersions(spec.Versions, limits)
		if err != nil {
			return nil, fmt.Errorf("kmtproto: capability %s: %w", spec.Name, err)
		}
		registry.versions[spec.Name] = versions
	}
	return registry, nil
}

func emptyCapabilityRegistry() *CapabilityRegistry {
	return &CapabilityRegistry{versions: make(map[string][]uint16)}
}

// Negotiate returns accepted capabilities in canonical name order. For each
// capability it selects the highest mutually supported version.
func (r *CapabilityRegistry) Negotiate(offers []CapabilityOffer, limits Limits) (SessionCapabilities, error) {
	limits = normalizeLimits(limits)
	validated, err := validateAndCopyCapabilityOffers(offers, limits)
	if err != nil {
		return SessionCapabilities{}, err
	}
	if r == nil {
		r = emptyCapabilityRegistry()
	}
	accepted := make([]NegotiatedCapability, 0, len(validated))
	for _, offer := range validated {
		serverVersions := r.versions[offer.Name]
		version, ok := highestCommonCapabilityVersion(offer.Versions, serverVersions)
		if !ok {
			if offer.Required {
				return SessionCapabilities{}, NewProtocolError(ErrorUnsupportedFeature, "required capability is unsupported: "+offer.Name)
			}
			continue
		}
		accepted = append(accepted, NegotiatedCapability{Name: offer.Name, Version: version})
	}
	sort.Slice(accepted, func(i, j int) bool { return accepted[i].Name < accepted[j].Name })
	return NewSessionCapabilities(accepted, limits)
}

func (r *CapabilityRegistry) validate(limits Limits) error {
	limits = normalizeLimits(limits)
	if r == nil {
		return nil
	}
	if len(r.versions) > limits.MaxCapabilities {
		return NewProtocolError(ErrorInvalidCapability, "capability registry exceeds configured limits")
	}
	for name, versions := range r.versions {
		if err := validateCapabilityName(name, limits); err != nil {
			return err
		}
		if len(versions) == 0 || len(versions) > limits.MaxCapabilityVersions {
			return NewProtocolError(ErrorInvalidCapability, "capability registry version list exceeds configured limits")
		}
	}
	return nil
}

// ValidateNegotiatedCapabilities validates the shape and uniqueness of a
// negotiated capability list. It does not validate it against a client offer.
func ValidateNegotiatedCapabilities(capabilities []NegotiatedCapability, limits Limits) error {
	limits = normalizeLimits(limits)
	if len(capabilities) > limits.MaxCapabilities {
		return NewProtocolError(ErrorInvalidCapability, "too many negotiated capabilities")
	}
	seen := make(map[string]struct{}, len(capabilities))
	previous := ""
	for i, capability := range capabilities {
		if err := validateCapabilityName(capability.Name, limits); err != nil {
			return err
		}
		if capability.Version == 0 {
			return NewProtocolError(ErrorInvalidCapability, "capability version must be positive")
		}
		if _, exists := seen[capability.Name]; exists {
			return NewProtocolError(ErrorInvalidCapability, "duplicate negotiated capability: "+capability.Name)
		}
		if i > 0 && capability.Name <= previous {
			return NewProtocolError(ErrorInvalidCapability, "negotiated capabilities must use canonical name order")
		}
		seen[capability.Name] = struct{}{}
		previous = capability.Name
	}
	return nil
}

func validateNegotiatedAgainstOffers(offers []CapabilityOffer, accepted []NegotiatedCapability, limits Limits) error {
	validated, err := validateAndCopyCapabilityOffers(offers, limits)
	if err != nil {
		return err
	}
	if err := ValidateNegotiatedCapabilities(accepted, limits); err != nil {
		return err
	}
	offerByName := make(map[string]CapabilityOffer, len(validated))
	for _, offer := range validated {
		offerByName[offer.Name] = offer
	}
	acceptedNames := make(map[string]struct{}, len(accepted))
	for _, capability := range accepted {
		offer, exists := offerByName[capability.Name]
		if !exists || !containsCapabilityVersion(offer.Versions, capability.Version) {
			return NewProtocolError(ErrorProtocolViolation, "server accepted an unoffered capability version")
		}
		acceptedNames[capability.Name] = struct{}{}
	}
	for _, offer := range validated {
		if offer.Required {
			if _, acceptedRequired := acceptedNames[offer.Name]; !acceptedRequired {
				return NewProtocolError(ErrorProtocolViolation, "server omitted a required capability: "+offer.Name)
			}
		}
	}
	return nil
}

func validateAndCopyCapabilityOffers(offers []CapabilityOffer, limits Limits) ([]CapabilityOffer, error) {
	limits = normalizeLimits(limits)
	if len(offers) > limits.MaxCapabilities {
		return nil, NewProtocolError(ErrorInvalidCapability, "too many capability offers")
	}
	seen := make(map[string]struct{}, len(offers))
	result := make([]CapabilityOffer, 0, len(offers))
	for _, offer := range offers {
		if err := validateCapabilityName(offer.Name, limits); err != nil {
			return nil, err
		}
		if _, exists := seen[offer.Name]; exists {
			return nil, NewProtocolError(ErrorInvalidCapability, "duplicate capability offer: "+offer.Name)
		}
		versions, err := validateAndSortCapabilityVersions(offer.Versions, limits)
		if err != nil {
			return nil, fmt.Errorf("kmtproto: capability %s: %w", offer.Name, err)
		}
		seen[offer.Name] = struct{}{}
		result = append(result, CapabilityOffer{Name: offer.Name, Versions: versions, Required: offer.Required})
	}
	return result, nil
}

func validateCapabilityName(name string, limits Limits) error {
	if name == "" || len(name) > limits.MaxCapabilityNameLength || !utf8.ValidString(name) {
		return NewProtocolError(ErrorInvalidCapability, "invalid or oversized capability name")
	}
	if name[0] < 'a' || name[0] > 'z' {
		return NewProtocolError(ErrorInvalidCapability, "invalid capability name: "+name)
	}
	separator := false
	for i := 1; i < len(name); i++ {
		ch := name[i]
		switch {
		case ch >= 'a' && ch <= 'z':
			separator = false
		case ch >= '0' && ch <= '9':
			separator = false
		case ch == '.' || ch == '-':
			if separator {
				return NewProtocolError(ErrorInvalidCapability, "invalid capability name: "+name)
			}
			separator = true
		default:
			return NewProtocolError(ErrorInvalidCapability, "invalid capability name: "+name)
		}
	}
	if separator {
		return NewProtocolError(ErrorInvalidCapability, "invalid capability name: "+name)
	}
	parts := strings.FieldsFunc(name, func(r rune) bool { return r == '.' || r == '-' })
	last := parts[len(parts)-1]
	if len(last) > 1 && last[0] == 'v' && allASCIIDigits(last[1:]) {
		return NewProtocolError(ErrorInvalidCapability, "capability version must use the numeric version field")
	}
	return nil
}

func allASCIIDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func validateAndSortCapabilityVersions(versions []uint16, limits Limits) ([]uint16, error) {
	if len(versions) == 0 || len(versions) > limits.MaxCapabilityVersions {
		return nil, NewProtocolError(ErrorInvalidCapability, "capability requires a bounded non-empty version list")
	}
	result := append([]uint16(nil), versions...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	for i, version := range result {
		if version == 0 {
			return nil, NewProtocolError(ErrorInvalidCapability, "capability version must be positive")
		}
		if i > 0 && version == result[i-1] {
			return nil, NewProtocolError(ErrorInvalidCapability, "duplicate capability version")
		}
	}
	return result, nil
}

func highestCommonCapabilityVersion(client, server []uint16) (uint16, bool) {
	i, j := len(client)-1, len(server)-1
	for i >= 0 && j >= 0 {
		switch {
		case client[i] == server[j]:
			return client[i], true
		case client[i] > server[j]:
			i--
		default:
			j--
		}
	}
	return 0, false
}

func containsCapabilityVersion(versions []uint16, target uint16) bool {
	i := sort.Search(len(versions), func(i int) bool { return versions[i] >= target })
	return i < len(versions) && versions[i] == target
}

func cloneCapabilityOffers(offers []CapabilityOffer) []CapabilityOffer {
	result := make([]CapabilityOffer, len(offers))
	for i, offer := range offers {
		result[i] = CapabilityOffer{Name: offer.Name, Versions: append([]uint16(nil), offer.Versions...), Required: offer.Required}
	}
	return result
}

func cloneNegotiatedCapabilities(capabilities []NegotiatedCapability) []NegotiatedCapability {
	return append([]NegotiatedCapability(nil), capabilities...)
}

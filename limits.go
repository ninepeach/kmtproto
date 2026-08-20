package kmtproto

import "errors"

const (
	DefaultMaxReplayEvents         uint64 = 4096
	DefaultMaxReplayBytes                 = 16 << 20
	DefaultEventIdentityWindow            = 4096
	DefaultMaxCapabilities                = 32
	DefaultMaxCapabilityNameLength        = 64
	DefaultMaxCapabilityVersions          = 16
	DefaultMaxClientNameLength            = 128
	DefaultMaxEventTypeLength             = 128
	DefaultMaxStateNamespaceLength        = 64
	DefaultMaxStateObjectIDLength         = 256
	DefaultMaxStateDataSize               = 512 << 10
	DefaultMaxStateObjectSize             = 640 << 10
	DefaultMaxStateQueryObjects           = 128
	DefaultMaxStateSnapshotObjects        = 128
	DefaultMaxStateSnapshotBytes          = 768 << 10
	DefaultMaxStateSyncNamespaces         = 32
	DefaultMaxStateCacheObjects           = 4096
	DefaultMaxStateCacheBytes             = 16 << 20
)

type Limits struct {
	MaxFrameSize            int
	MaxPayloadSize          int
	MaxIDLength             int
	MaxSessionIDLength      int
	MaxErrorMessageLength   int
	MaxCapabilities         int
	MaxCapabilityNameLength int
	MaxCapabilityVersions   int
	MaxClientNameLength     int
	MaxEventTypeLength      int
	MaxStateNamespaceLength int
	MaxStateObjectIDLength  int
	MaxStateDataSize        int
	MaxStateObjectSize      int
	MaxStateQueryObjects    int
	MaxStateSnapshotObjects int
	MaxStateSnapshotBytes   int
	MaxStateSyncNamespaces  int
}

func DefaultLimits() Limits {
	return Limits{
		MaxFrameSize:            1 << 20,
		MaxPayloadSize:          768 << 10,
		MaxIDLength:             128,
		MaxSessionIDLength:      128,
		MaxErrorMessageLength:   1024,
		MaxCapabilities:         DefaultMaxCapabilities,
		MaxCapabilityNameLength: DefaultMaxCapabilityNameLength,
		MaxCapabilityVersions:   DefaultMaxCapabilityVersions,
		MaxClientNameLength:     DefaultMaxClientNameLength,
		MaxEventTypeLength:      DefaultMaxEventTypeLength,
		MaxStateNamespaceLength: DefaultMaxStateNamespaceLength,
		MaxStateObjectIDLength:  DefaultMaxStateObjectIDLength,
		MaxStateDataSize:        DefaultMaxStateDataSize,
		MaxStateObjectSize:      DefaultMaxStateObjectSize,
		MaxStateQueryObjects:    DefaultMaxStateQueryObjects,
		MaxStateSnapshotObjects: DefaultMaxStateSnapshotObjects,
		MaxStateSnapshotBytes:   DefaultMaxStateSnapshotBytes,
		MaxStateSyncNamespaces:  DefaultMaxStateSyncNamespaces,
	}
}

func normalizeLimits(limits Limits) Limits {
	defaults := DefaultLimits()
	if limits.MaxFrameSize == 0 {
		limits.MaxFrameSize = defaults.MaxFrameSize
	}
	if limits.MaxPayloadSize == 0 {
		limits.MaxPayloadSize = defaults.MaxPayloadSize
	}
	if limits.MaxIDLength == 0 {
		limits.MaxIDLength = defaults.MaxIDLength
	}
	if limits.MaxSessionIDLength == 0 {
		limits.MaxSessionIDLength = defaults.MaxSessionIDLength
	}
	if limits.MaxErrorMessageLength == 0 {
		limits.MaxErrorMessageLength = defaults.MaxErrorMessageLength
	}
	if limits.MaxCapabilities == 0 {
		limits.MaxCapabilities = DefaultMaxCapabilities
	}
	if limits.MaxCapabilityNameLength == 0 {
		limits.MaxCapabilityNameLength = DefaultMaxCapabilityNameLength
	}
	if limits.MaxCapabilityVersions == 0 {
		limits.MaxCapabilityVersions = DefaultMaxCapabilityVersions
	}
	if limits.MaxClientNameLength == 0 {
		limits.MaxClientNameLength = DefaultMaxClientNameLength
	}
	if limits.MaxEventTypeLength == 0 {
		limits.MaxEventTypeLength = DefaultMaxEventTypeLength
	}
	if limits.MaxStateNamespaceLength == 0 {
		limits.MaxStateNamespaceLength = DefaultMaxStateNamespaceLength
	}
	if limits.MaxStateObjectIDLength == 0 {
		limits.MaxStateObjectIDLength = DefaultMaxStateObjectIDLength
	}
	if limits.MaxStateDataSize == 0 {
		limits.MaxStateDataSize = DefaultMaxStateDataSize
	}
	if limits.MaxStateObjectSize == 0 {
		limits.MaxStateObjectSize = DefaultMaxStateObjectSize
	}
	if limits.MaxStateQueryObjects == 0 {
		limits.MaxStateQueryObjects = DefaultMaxStateQueryObjects
	}
	if limits.MaxStateSnapshotObjects == 0 {
		limits.MaxStateSnapshotObjects = DefaultMaxStateSnapshotObjects
	}
	if limits.MaxStateSnapshotBytes == 0 {
		limits.MaxStateSnapshotBytes = DefaultMaxStateSnapshotBytes
	}
	if limits.MaxStateSyncNamespaces == 0 {
		limits.MaxStateSyncNamespaces = DefaultMaxStateSyncNamespaces
	}
	return limits
}

func validateLimits(limits Limits) error {
	if limits.MaxFrameSize <= 0 || limits.MaxPayloadSize <= 0 || limits.MaxIDLength <= 0 ||
		limits.MaxSessionIDLength <= 0 || limits.MaxErrorMessageLength <= 0 ||
		limits.MaxCapabilities <= 0 || limits.MaxCapabilityNameLength <= 0 ||
		limits.MaxCapabilityVersions <= 0 || limits.MaxClientNameLength <= 0 ||
		limits.MaxEventTypeLength <= 0 || limits.MaxStateNamespaceLength <= 0 ||
		limits.MaxStateObjectIDLength <= 0 || limits.MaxStateDataSize <= 0 ||
		limits.MaxStateObjectSize <= 0 || limits.MaxStateQueryObjects <= 0 ||
		limits.MaxStateSnapshotObjects <= 0 || limits.MaxStateSnapshotBytes <= 0 ||
		limits.MaxStateSyncNamespaces <= 0 {
		return errors.New("kmtproto: protocol limits must be positive")
	}
	return nil
}

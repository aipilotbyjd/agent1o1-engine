package events

import (
	"encoding/json"
	"fmt"

	"github.com/linkflow/engine/internal/history/types"
)

// CurrentSchemaVersion is the current event schema version.
const CurrentSchemaVersion = 1

// VersionedEvent wraps a serialized event with schema version metadata.
type VersionedEvent struct {
	SchemaVersion int             `json:"schema_version"`
	Data          json.RawMessage `json:"data"`
}

// SerializeVersioned serializes a history event with schema version metadata.
func (s *Serializer) SerializeVersioned(event *types.HistoryEvent) ([]byte, error) {
	data, err := s.Serialize(event)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize event: %w", err)
	}

	versioned := VersionedEvent{
		SchemaVersion: CurrentSchemaVersion,
		Data:          json.RawMessage(data),
	}

	return json.Marshal(versioned)
}

// DeserializeVersioned deserializes a versioned event, handling schema migration.
func (s *Serializer) DeserializeVersioned(data []byte) (*types.HistoryEvent, error) {
	var versioned VersionedEvent
	if err := json.Unmarshal(data, &versioned); err != nil {
		// Not a versioned event - treat as legacy (current format)
		return s.Deserialize(data)
	}

	// If schema_version is missing or 0, treat as legacy
	if versioned.SchemaVersion == 0 {
		return s.Deserialize(data)
	}

	switch versioned.SchemaVersion {
	case 1:
		return s.Deserialize([]byte(versioned.Data))
	default:
		return nil, fmt.Errorf("unsupported schema version: %d", versioned.SchemaVersion)
	}
}

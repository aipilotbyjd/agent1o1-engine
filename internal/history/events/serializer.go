package events

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/linkflow/engine/internal/history/types"
)

func init() {
	gob.Register(&types.ExecutionStartedAttributes{})
	gob.Register(&types.ExecutionCompletedAttributes{})
	gob.Register(&types.ExecutionFailedAttributes{})
	gob.Register(&types.ExecutionTerminatedAttributes{})
	gob.Register(&types.NodeScheduledAttributes{})
	gob.Register(&types.NodeStartedAttributes{})
	gob.Register(&types.NodeCompletedAttributes{})
	gob.Register(&types.NodeFailedAttributes{})
	gob.Register(&types.TimerStartedAttributes{})
	gob.Register(&types.TimerFiredAttributes{})
	gob.Register(&types.TimerCanceledAttributes{})
	gob.Register(&types.ActivityScheduledAttributes{})
	gob.Register(&types.ActivityStartedAttributes{})
	gob.Register(&types.ActivityCompletedAttributes{})
	gob.Register(&types.ActivityFailedAttributes{})
	gob.Register(&types.SignalReceivedAttributes{})
	gob.Register(&types.MarkerRecordedAttributes{})
	gob.Register(&types.ExecutionKey{})
	gob.Register(&types.RetryPolicy{})
}

type EncodingType int

const (
	EncodingTypeJSON EncodingType = iota
	EncodingTypeGob
)

const currentSerializerVersion = 1

type Serializer struct {
	encoding EncodingType
}

func NewSerializer(encoding EncodingType) *Serializer {
	return &Serializer{
		encoding: encoding,
	}
}

func NewJSONSerializer() *Serializer {
	return NewSerializer(EncodingTypeJSON)
}

func NewGobSerializer() *Serializer {
	return NewSerializer(EncodingTypeGob)
}

type serializedEvent struct {
	Version    int                    `json:"v"`
	EventID    int64                  `json:"event_id"`
	EventType  int32                  `json:"event_type"`
	Timestamp  int64                  `json:"timestamp"`
	EvtVersion int64                  `json:"evt_version"`
	TaskID     int64                  `json:"task_id"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

func (s *Serializer) Serialize(event *types.HistoryEvent) ([]byte, error) {
	if event == nil {
		return nil, errors.New("cannot serialize nil event")
	}

	switch s.encoding {
	case EncodingTypeJSON:
		return s.serializeJSON(event)
	case EncodingTypeGob:
		return s.serializeGob(event)
	default:
		return nil, fmt.Errorf("unsupported encoding type: %d", s.encoding)
	}
}

func (s *Serializer) serializeJSON(event *types.HistoryEvent) ([]byte, error) {
	se := serializedEvent{
		Version:    currentSerializerVersion,
		EventID:    event.EventID,
		EventType:  int32(event.EventType),
		Timestamp:  event.Timestamp.UnixNano(),
		EvtVersion: event.Version,
		TaskID:     event.TaskID,
	}

	if event.Attributes != nil {
		attrBytes, err := json.Marshal(event.Attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal attributes: %w", err)
		}
		var attrMap map[string]interface{}
		if err := json.Unmarshal(attrBytes, &attrMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal attributes to map: %w", err)
		}
		se.Attributes = attrMap
	}

	return json.Marshal(se)
}

func (s *Serializer) serializeGob(event *types.HistoryEvent) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(byte(currentSerializerVersion))
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(event); err != nil {
		return nil, fmt.Errorf("failed to gob encode event: %w", err)
	}
	return buf.Bytes(), nil
}

func (s *Serializer) Deserialize(data []byte) (*types.HistoryEvent, error) {
	if len(data) == 0 {
		return nil, errors.New("cannot deserialize empty data")
	}

	switch s.encoding {
	case EncodingTypeJSON:
		return s.deserializeJSON(data)
	case EncodingTypeGob:
		return s.deserializeGob(data)
	default:
		return nil, fmt.Errorf("unsupported encoding type: %d", s.encoding)
	}
}

func (s *Serializer) deserializeJSON(data []byte) (*types.HistoryEvent, error) {
	var se serializedEvent
	if err := json.Unmarshal(data, &se); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}

	event := &types.HistoryEvent{
		EventID:   se.EventID,
		EventType: types.EventType(se.EventType),
		Version:   se.EvtVersion,
		TaskID:    se.TaskID,
	}
	event.Timestamp = time.Unix(0, se.Timestamp).UTC()

	if se.Attributes != nil {
		attrs, err := s.deserializeAttributes(types.EventType(se.EventType), se.Attributes)
		if err != nil {
			return nil, err
		}
		event.Attributes = attrs
	}

	return event, nil
}

func (s *Serializer) deserializeAttributes(eventType types.EventType, attrMap map[string]interface{}) (any, error) {
	attrBytes, err := json.Marshal(attrMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal attribute map: %w", err)
	}

	var attrs any
	switch eventType {
	case types.EventTypeExecutionStarted:
		attrs = &types.ExecutionStartedAttributes{}
	case types.EventTypeExecutionCompleted:
		attrs = &types.ExecutionCompletedAttributes{}
	case types.EventTypeExecutionFailed:
		attrs = &types.ExecutionFailedAttributes{}
	case types.EventTypeExecutionTerminated:
		attrs = &types.ExecutionTerminatedAttributes{}
	case types.EventTypeNodeScheduled:
		attrs = &types.NodeScheduledAttributes{}
	case types.EventTypeNodeStarted:
		attrs = &types.NodeStartedAttributes{}
	case types.EventTypeNodeCompleted:
		attrs = &types.NodeCompletedAttributes{}
	case types.EventTypeNodeFailed:
		attrs = &types.NodeFailedAttributes{}
	case types.EventTypeTimerStarted:
		attrs = &types.TimerStartedAttributes{}
	case types.EventTypeTimerFired:
		attrs = &types.TimerFiredAttributes{}
	case types.EventTypeTimerCanceled:
		attrs = &types.TimerCanceledAttributes{}
	case types.EventTypeActivityScheduled:
		attrs = &types.ActivityScheduledAttributes{}
	case types.EventTypeActivityStarted:
		attrs = &types.ActivityStartedAttributes{}
	case types.EventTypeActivityCompleted:
		attrs = &types.ActivityCompletedAttributes{}
	case types.EventTypeActivityFailed:
		attrs = &types.ActivityFailedAttributes{}
	case types.EventTypeSignalReceived:
		attrs = &types.SignalReceivedAttributes{}
	case types.EventTypeMarkerRecorded:
		attrs = &types.MarkerRecordedAttributes{}
	default:
		return attrMap, nil
	}

	if err := json.Unmarshal(attrBytes, attrs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal attributes for event type %s: %w", eventType, err)
	}

	return attrs, nil
}

func (s *Serializer) deserializeGob(data []byte) (*types.HistoryEvent, error) {
	if len(data) < 2 {
		return nil, errors.New("gob data too short")
	}

	buf := bytes.NewBuffer(data[1:])
	dec := gob.NewDecoder(buf)

	var event types.HistoryEvent
	if err := dec.Decode(&event); err != nil {
		return nil, fmt.Errorf("failed to gob decode event: %w", err)
	}

	return &event, nil
}

func (s *Serializer) SerializeEvents(events []*types.HistoryEvent) ([][]byte, error) {
	result := make([][]byte, len(events))
	for i, event := range events {
		data, err := s.Serialize(event)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize event %d: %w", event.EventID, err)
		}
		result[i] = data
	}
	return result, nil
}

func (s *Serializer) DeserializeEvents(dataList [][]byte) ([]*types.HistoryEvent, error) {
	result := make([]*types.HistoryEvent, len(dataList))
	for i, data := range dataList {
		event, err := s.Deserialize(data)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize event at index %d: %w", i, err)
		}
		result[i] = event
	}
	return result, nil
}

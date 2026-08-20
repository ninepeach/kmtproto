package kmtproto

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestNewStateObject(t *testing.T) {
	data := json.RawMessage(`{"status":"read"}`)
	object, err := NewStateObject("message", "msg001", 5, data, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if object.Namespace != "message" || object.ObjectID != "msg001" || object.Version != 5 || string(object.Data) != string(data) {
		t.Fatalf("unexpected State Object: %#v", object)
	}
	if object.Identity() != (StateIdentity{Namespace: "message", ObjectID: "msg001"}) {
		t.Fatalf("unexpected State identity: %#v", object.Identity())
	}
	data[0] = '['
	if string(object.Data) != `{"status":"read"}` {
		t.Fatal("State Object retained mutable input data")
	}
	if _, err := NewStateObject("message", "nullable", 1, json.RawMessage(`null`), DefaultLimits()); err != nil {
		t.Fatalf("valid JSON null State data was rejected: %v", err)
	}
}

func TestValidateStateObjectRejectsInvalidInput(t *testing.T) {
	valid := func() StateObject {
		return StateObject{Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`)}
	}
	tests := []struct {
		name   string
		mutate func(*StateObject, *Limits)
	}{
		{name: "empty namespace", mutate: func(object *StateObject, _ *Limits) { object.Namespace = "" }},
		{name: "uppercase namespace", mutate: func(object *StateObject, _ *Limits) { object.Namespace = "Message" }},
		{name: "invalid namespace separator", mutate: func(object *StateObject, _ *Limits) { object.Namespace = "message..status" }},
		{name: "oversized namespace", mutate: func(object *StateObject, limits *Limits) { limits.MaxStateNamespaceLength = 3 }},
		{name: "empty object id", mutate: func(object *StateObject, _ *Limits) { object.ObjectID = "" }},
		{name: "invalid utf8 object id", mutate: func(object *StateObject, _ *Limits) { object.ObjectID = string([]byte{0xff}) }},
		{name: "control character object id", mutate: func(object *StateObject, _ *Limits) { object.ObjectID = "msg\n001" }},
		{name: "oversized object id", mutate: func(object *StateObject, limits *Limits) { limits.MaxStateObjectIDLength = 3 }},
		{name: "zero version", mutate: func(object *StateObject, _ *Limits) { object.Version = 0 }},
		{name: "exhausted version", mutate: func(object *StateObject, _ *Limits) { object.Version = math.MaxUint64 }},
		{name: "missing data", mutate: func(object *StateObject, _ *Limits) { object.Data = nil }},
		{name: "invalid data", mutate: func(object *StateObject, _ *Limits) { object.Data = json.RawMessage(`{"open":`) }},
		{name: "oversized data", mutate: func(object *StateObject, limits *Limits) {
			limits.MaxStateDataSize = 4
			object.Data = json.RawMessage(`{"a":1}`)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			object := valid()
			limits := DefaultLimits()
			test.mutate(&object, &limits)
			if err := ValidateStateObject(&object, limits); err == nil {
				t.Fatal("expected State Object validation failure")
			}
		})
	}
	if err := ValidateStateObject(nil, DefaultLimits()); err == nil {
		t.Fatal("nil State Object was accepted")
	}
}

func TestApplyStateObjectAcceptsNewerVersion(t *testing.T) {
	current := StateObject{Namespace: "message", ObjectID: "msg001", Version: 5, Data: json.RawMessage(`{"status":"read"}`)}
	incoming := StateObject{Namespace: "message", ObjectID: "msg001", Version: 8, Data: json.RawMessage(`{"status":"archived"}`)}
	committed, result, err := ApplyStateObject(&current, incoming, DefaultLimits())
	if err != nil || result != StateApplyApplied || committed.Version != 8 || string(committed.Data) != string(incoming.Data) {
		t.Fatalf("newer State was not applied: committed=%#v result=%s err=%v", committed, result, err)
	}
	committed.Data[0] = '['
	if string(incoming.Data) != `{"status":"archived"}` {
		t.Fatal("applied State exposed mutable incoming data")
	}
	if current.Version != 5 || string(current.Data) != `{"status":"read"}` {
		t.Fatal("pure State apply mutated current object")
	}
}

func TestApplyStateObjectRejectsStaleVersion(t *testing.T) {
	current := StateObject{Namespace: "task", ObjectID: "pickup001", Version: 6, Data: json.RawMessage(`{"status":"completed"}`)}
	incoming := StateObject{Namespace: "task", ObjectID: "pickup001", Version: 5, Data: json.RawMessage(`{"status":"pending"}`)}
	committed, result, err := ApplyStateObject(&current, incoming, DefaultLimits())
	if !errors.Is(err, ErrStateStale) || result != StateApplyStale {
		t.Fatalf("stale State returned result=%s err=%v", result, err)
	}
	if committed.Version != current.Version || string(committed.Data) != string(current.Data) {
		t.Fatalf("stale State overwrote current value: %#v", committed)
	}
}

func TestApplyStateObjectSameVersionDuplicateAndConflict(t *testing.T) {
	current := StateObject{
		Namespace: "message",
		ObjectID:  "msg001",
		Version:   5,
		Data:      json.RawMessage(`{"count":1,"nested":{"b":2,"a":"\u0061"}}`),
	}
	semanticDuplicate := StateObject{
		Namespace: "message",
		ObjectID:  "msg001",
		Version:   5,
		Data:      json.RawMessage(` { "nested": {"a":"a", "b":2.0}, "count":1e+0 } `),
	}
	committed, result, err := ApplyStateObject(&current, semanticDuplicate, DefaultLimits())
	if err != nil || result != StateApplyDuplicate || committed.Version != current.Version {
		t.Fatalf("equivalent same-version State was not a duplicate: result=%s err=%v", result, err)
	}

	conflict := semanticDuplicate
	conflict.Data = json.RawMessage(`{"count":2,"nested":{"a":"a","b":2}}`)
	committed, result, err = ApplyStateObject(&current, conflict, DefaultLimits())
	if !errors.Is(err, ErrStateConflict) || result != StateApplyConflict {
		t.Fatalf("different same-version State returned result=%s err=%v", result, err)
	}
	if string(committed.Data) != string(current.Data) {
		t.Fatal("same-version conflict overwrote current State")
	}

	otherIdentity := current
	otherIdentity.ObjectID = "msg002"
	if _, result, err = ApplyStateObject(&current, otherIdentity, DefaultLimits()); !errors.Is(err, ErrStateIdentityMismatch) || result != StateApplyConflict {
		t.Fatalf("identity mismatch returned result=%s err=%v", result, err)
	}
	if got := StateApplyResult(255).String(); got != "UNKNOWN" {
		t.Fatalf("unknown StateApplyResult string=%q", got)
	}
}

func TestStateVersionDoesNotAffectEventSequence(t *testing.T) {
	client, _, generation := readyClient(t)
	event := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "evt_1", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(generation, event); err != nil {
		t.Fatal(err)
	}
	if client.LastSeq() != 1 {
		t.Fatalf("EVENT did not establish expected sequence: %d", client.LastSeq())
	}

	state, result, err := ApplyStateObject(nil, StateObject{Namespace: "message", ObjectID: "msg001", Version: 100, Data: json.RawMessage(`{"status":"read"}`)}, DefaultLimits())
	if err != nil || result != StateApplyApplied {
		t.Fatalf("initial State apply failed: result=%s err=%v", result, err)
	}
	if _, result, err = ApplyStateObject(&state, StateObject{Namespace: "message", ObjectID: "msg001", Version: 200, Data: json.RawMessage(`{"status":"archived"}`)}, DefaultLimits()); err != nil || result != StateApplyApplied {
		t.Fatalf("newer State apply failed: result=%s err=%v", result, err)
	}
	if client.LastSeq() != 1 || client.State() != StateReady {
		t.Fatalf("State version changed EVENT protocol state: seq=%d state=%s", client.LastSeq(), client.State())
	}
}

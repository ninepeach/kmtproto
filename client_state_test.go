package kmtproto

import (
	"encoding/json"
	"testing"
)

func TestStateUpdateNeverChangesEventSequenceInvariant(t *testing.T) {
	client, _, generation := readyStateSyncClient(t)
	update := Envelope{
		V: WireVersionV2, Type: FrameStateUpdate, ID: "state_1", SessionID: "s_state",
		Payload: mustPayload(StateUpdatePayload{State: StateObject{Namespace: "message", ObjectID: "m", Version: 1, Data: json.RawMessage(`{}`)}}),
	}
	if _, err := client.HandleIncoming(generation, update); err != nil {
		t.Fatal(err)
	}
	if client.LastSeq() != 0 {
		t.Fatalf("STATE changed EVENT sequence: %d", client.LastSeq())
	}
}

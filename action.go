package kmtproto

type Action interface{ isAction() }

type SendFrameAction struct{ Frame Envelope }
type DeliverEventAction struct{ Event Envelope }
type AckedAction struct{ MessageID string }
type SessionReadyAction struct{ SessionID string }
type StateChangedAction struct {
	State  StateObject
	Result StateApplyResult
}
type ProtocolErrorAction struct{ Error ErrorPayload }
type FullSyncRequiredAction struct{ SessionID string }
type CloseConnectionAction struct{ Reason string }

func (SendFrameAction) isAction()        {}
func (DeliverEventAction) isAction()     {}
func (AckedAction) isAction()            {}
func (SessionReadyAction) isAction()     {}
func (StateChangedAction) isAction()     {}
func (ProtocolErrorAction) isAction()    {}
func (FullSyncRequiredAction) isAction() {}
func (CloseConnectionAction) isAction()  {}

func copyEnvelope(e Envelope) Envelope {
	e.Payload = append([]byte(nil), e.Payload...)
	return e
}

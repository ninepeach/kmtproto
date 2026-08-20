package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ninepeach/kmtproto"
)

type assistantApp struct{}

func (assistantApp) HandleSend(_ context.Context, idempotencyKey string, payload []byte) error {
	fmt.Printf("application accepted SEND id=%s content=%s\n", idempotencyKey, payload)
	return nil
}

func main() {
	ctx := context.Background()
	clock := kmtproto.NewFakeClock(time.Unix(1_786_821_000, 0))

	sessions := kmtproto.NewMemorySessionRepository()
	dedup := kmtproto.NewMemoryDedupStore(clock, 24*time.Hour)
	replay := kmtproto.NewMemoryReplayStore()
	serverConfig := kmtproto.DefaultServerConfig()
	serverConfig.Clock = clock
	serverConfig.NewSessionID = func() (string, error) { return "s_demo", nil }
	server, mustErr := kmtproto.NewServerProtocol(serverConfig, kmtproto.ServerDependencies{
		Sessions:    sessions,
		Dedup:       dedup,
		Replay:      replay,
		Appender:    replay,
		Application: assistantApp{},
	})
	must(mustErr)

	clientConfig := kmtproto.DefaultClientConfig()
	clientConfig.Clock = clock
	client, mustErr := kmtproto.NewClientProtocol(clientConfig)
	must(mustErr)

	admission := kmtproto.NewServerAdmission()
	serverGeneration, serverOutbound := admission.Replace()
	clientGeneration := client.BeginConnect()
	must(client.TransportConnected(clientGeneration))

	helloActions, mustErr := client.StartSession(clientGeneration, "example-client")
	must(mustErr)
	must(admission.Handle(ctx, server, serverGeneration, sendFrame(helloActions)))
	welcome := next(ctx, serverOutbound)
	readyActions, mustErr := client.HandleIncoming(clientGeneration, welcome)
	must(mustErr)
	fmt.Printf("session ready: %s (%d action)\n", client.SessionID(), len(readyActions))

	sendActions, mustErr := client.EnqueueSend("msg_01", json.RawMessage(`{"text":"hello protocol"}`))
	must(mustErr)
	must(admission.Handle(ctx, server, serverGeneration, sendFrame(sendActions)))
	ack := next(ctx, serverOutbound)
	acked, mustErr := client.HandleIncoming(clientGeneration, ack)
	must(mustErr)
	fmt.Printf("SEND acknowledged (%d action)\n", len(acked))

	must(server.PublishEvent(client.SessionID(), "evt_01", "message.new", json.RawMessage(`{"text":"hello client"}`), serverOutbound))
	event := next(ctx, serverOutbound)
	delivered, mustErr := client.HandleIncoming(clientGeneration, event)
	must(mustErr)
	fmt.Printf("delivered EVENT seq=%d (%d action)\n", client.LastSeq(), len(delivered))

	// Lose the transport. An event produced while offline remains in ReplayStore.
	must(client.Disconnect(clientGeneration))
	must(server.PublishEvent(client.SessionID(), "evt_02", "message.new", json.RawMessage(`{"text":"while offline"}`), serverOutbound))

	serverGeneration, serverOutbound = admission.Replace()
	clientGeneration = client.BeginConnect()
	must(client.TransportConnected(clientGeneration))
	resumeActions, mustErr := client.Resume(clientGeneration)
	must(mustErr)
	must(admission.Handle(ctx, server, serverGeneration, sendFrame(resumeActions)))

	resumeWelcome := next(ctx, serverOutbound)
	_, mustErr = client.HandleIncoming(clientGeneration, resumeWelcome)
	must(mustErr)
	replayed := next(ctx, serverOutbound)
	recovered, mustErr := client.HandleIncoming(clientGeneration, replayed)
	must(mustErr)
	fmt.Printf("resume complete: state=%s last_seq=%d (%d actions)\n", client.State(), client.LastSeq(), len(recovered))
}

func sendFrame(actions []kmtproto.Action) kmtproto.Envelope {
	for _, action := range actions {
		if send, ok := action.(kmtproto.SendFrameAction); ok {
			return send.Frame
		}
	}
	panic("example: no SendFrameAction")
}

func next(ctx context.Context, queue *kmtproto.OutboundQueue) kmtproto.Envelope {
	frame, err := queue.Next(ctx)
	must(err)
	return frame
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

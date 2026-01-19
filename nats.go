package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/natscontext"
)

// ConnectNATS establishes a connection to NATS using the specified context
func ConnectNATS(contextName string) (*nats.Conn, jetstream.JetStream, error) {
	nc, _, err := natscontext.Connect(contextName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect: %w", err)
	}

	js, err := jetstream.New(nc, jetstream.WithDefaultTimeout(5*time.Minute))
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return nc, js, nil
}

// GetLimitsStreams returns all streams with limits retention policy
// If streamFilter is non-empty, only returns that specific stream if it has limits retention
func GetLimitsStreams(ctx context.Context, js jetstream.JetStream, streamFilter string) ([]jetstream.Stream, error) {
	var limitsStreams []jetstream.Stream

	// If filtering for a specific stream
	if streamFilter != "" {
		stream, err := js.Stream(ctx, streamFilter)
		if err != nil {
			return nil, fmt.Errorf("stream %q not found: %w", streamFilter, err)
		}

		info := stream.CachedInfo()
		if info.Config.Retention == jetstream.LimitsPolicy {
			limitsStreams = append(limitsStreams, stream)
		} else {
			return nil, fmt.Errorf("stream %q has %s retention policy, not limits", streamFilter, retentionPolicyName(info.Config.Retention))
		}

		return limitsStreams, nil
	}

	// List all streams and filter for limits retention
	streamLister := js.ListStreams(ctx)

	i := 1
	for stream := range streamLister.Info() {
		if stream.Config.Retention == jetstream.LimitsPolicy {
			s, err := js.Stream(ctx, stream.Config.Name)
			if err != nil {
				fmt.Printf("Warning: failed to get stream %s: %v\n", stream.Config.Name, err)
				continue
			}
			limitsStreams = append(limitsStreams, s)
			i++
		}
	}
	println()

	if err := streamLister.Err(); err != nil {
		return nil, fmt.Errorf("error listing streams: %w", err)
	}

	return limitsStreams, nil
}

// retentionPolicyName returns a human-readable name for the retention policy
func retentionPolicyName(policy jetstream.RetentionPolicy) string {
	switch policy {
	case jetstream.LimitsPolicy:
		return "limits"
	case jetstream.InterestPolicy:
		return "interest"
	case jetstream.WorkQueuePolicy:
		return "workqueue"
	default:
		return "unknown"
	}
}

package main

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/natscontext"
)

// StreamInfo holds stream metadata including sequence and timestamp bounds
type StreamInfo struct {
	Stream         jetstream.Stream
	Name           string
	FirstSeq       uint64
	LastSeq        uint64
	FirstTimestamp time.Time
	LastTimestamp  time.Time
	MsgCount       uint64
}

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

// GetLimitsStreams returns all streams with limits retention policy along with their metadata
// If streamFilters is non-empty, only returns matching streams
func GetLimitsStreams(ctx context.Context, js jetstream.JetStream, streamFilters []string, showProgress bool) ([]StreamInfo, error) {
	var streamInfos []StreamInfo

	// Helper to add a stream with its metadata
	addStream := func(stream jetstream.Stream) error {
		info := stream.CachedInfo()
		if info.Config.Retention != jetstream.LimitsPolicy {
			return fmt.Errorf("stream %q has %s retention policy, not limits", info.Config.Name, retentionPolicyName(info.Config.Retention))
		}

		if info.State.Msgs == 0 {
			// Skip empty streams
			return nil
		}

		si := StreamInfo{
			Stream:         stream,
			Name:           info.Config.Name,
			FirstSeq:       info.State.FirstSeq,
			LastSeq:        info.State.LastSeq,
			MsgCount:       info.State.Msgs,
			FirstTimestamp: info.State.FirstTime,
			LastTimestamp:  info.State.LastTime,
		}

		streamInfos = append(streamInfos, si)
		return nil
	}

	// List all streams and filter for limits retention
	streamLister := js.ListStreams(ctx)

	i := 1
	for streamInfo := range streamLister.Info() {
		if (len(streamFilters) == 0 || slices.Contains(streamFilters, streamInfo.Config.Name)) && streamInfo.Config.Retention == jetstream.LimitsPolicy {
			if showProgress {
				fmt.Printf("Found stream %d: %s\r", i, streamInfo.Config.Name)
			}
			stream, err := js.Stream(ctx, streamInfo.Config.Name)
			if err != nil {
				fmt.Printf("Warning: failed to get stream %s: %v\n", streamInfo.Config.Name, err)
				continue
			}
			if err := addStream(stream); err != nil {
				fmt.Printf("Warning: %v\n", err)
				continue
			}
			i++
		}
	}
	if showProgress {
		fmt.Print("\r                                                            \r")
	}

	if err := streamLister.Err(); err != nil {
		return nil, fmt.Errorf("error listing streams: %w", err)
	}

	return streamInfos, nil
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

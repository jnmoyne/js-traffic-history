package main

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

// MessageData holds the relevant data extracted from a message
type MessageData struct {
	StreamName string
	Sequence   uint64
	Timestamp  time.Time
	Size       int // message payload size in bytes
}

// ProgressFunc is called to report fetch progress (current, total)
type ProgressFunc func(current, total int)

// FetchStreamMessages retrieves messages from a stream using jetstreamext.GetBatch
// If startTime is specified, fetching starts from that time. If endTime is specified,
// fetching stops when messages exceed that time.
func FetchStreamMessages(ctx context.Context, js jetstream.JetStream, stream jetstream.Stream, batchSize, limit int, startTime, endTime *time.Time, progress ProgressFunc) ([]MessageData, error) {
	info := stream.CachedInfo()
	streamName := info.Config.Name
	firstSeq := info.State.FirstSeq

	if info.State.Msgs == 0 {
		return nil, nil
	}

	// Determine how many messages to fetch (this is an upper bound estimate)
	totalToFetch := int(info.State.Msgs)
	if limit > 0 && limit < totalToFetch {
		totalToFetch = limit
	}

	messages := make([]MessageData, 0, totalToFetch)
	currentSeq := firstSeq
	useStartTime := startTime != nil // Use start time for the first batch only

	for limit == 0 || len(messages) < limit {
		// Calculate fetch size
		fetchSize := batchSize
		if limit > 0 {
			remaining := limit - len(messages)
			if remaining < batchSize {
				fetchSize = remaining
			}
		}

		// Build options for GetBatch
		var opts []jetstreamext.GetBatchOpt
		if useStartTime {
			opts = append(opts, jetstreamext.GetBatchStartTime(*startTime))
			useStartTime = false // Only use start time for the first batch
		} else {
			opts = append(opts, jetstreamext.GetBatchSeq(currentSeq))
		}

		// Fetch batch using GetBatch
		msgIter, err := jetstreamext.GetBatch(ctx, js, streamName, fetchSize, opts...)
		if err != nil {
			return messages, err
		}

		batchCount := 0
		var lastSeq uint64
		hitEndTime := false
		for msg, err := range msgIter {
			if err != nil {
				// Skip errors (message might have been deleted)
				continue
			}

			// Check if message is past end time
			if endTime != nil && msg.Time.After(*endTime) {
				hitEndTime = true
				break
			}

			messages = append(messages, MessageData{
				StreamName: streamName,
				Sequence:   msg.Sequence,
				Timestamp:  msg.Time,
				Size:       len(msg.Data),
			})
			lastSeq = msg.Sequence
			batchCount++

			if progress != nil {
				progress(len(messages), totalToFetch)
			}

			// Check if we've hit the limit
			if limit > 0 && len(messages) >= limit {
				break
			}
		}

		// Stop if we hit end time or no messages were fetched
		if hitEndTime || batchCount == 0 {
			break
		}

		// Move to next sequence after the last fetched message
		currentSeq = lastSeq + 1
	}

	// Trim to limit if needed
	if limit > 0 && len(messages) > limit {
		messages = messages[:limit]
	}

	return messages, nil
}

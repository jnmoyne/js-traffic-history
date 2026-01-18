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
func FetchStreamMessages(ctx context.Context, js jetstream.JetStream, stream jetstream.Stream, batchSize, limit int, progress ProgressFunc) ([]MessageData, error) {
	info := stream.CachedInfo()
	streamName := info.Config.Name
	firstSeq := info.State.FirstSeq

	if info.State.Msgs == 0 {
		return nil, nil
	}

	// Determine how many messages to fetch
	totalToFetch := int(info.State.Msgs)
	if limit > 0 && limit < totalToFetch {
		totalToFetch = limit
	}

	messages := make([]MessageData, 0, totalToFetch)
	currentSeq := firstSeq

	for len(messages) < totalToFetch {
		// Calculate remaining messages to fetch
		remaining := totalToFetch - len(messages)
		fetchSize := batchSize
		if remaining < batchSize {
			fetchSize = remaining
		}

		// Fetch batch using GetBatch
		msgIter, err := jetstreamext.GetBatch(ctx, js, streamName, fetchSize, jetstreamext.GetBatchSeq(currentSeq))
		if err != nil {
			return messages, err
		}

		batchCount := 0
		var lastSeq uint64
		for msg, err := range msgIter {
			if err != nil {
				// Skip errors (message might have been deleted)
				continue
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

		// If no messages were fetched in this batch, we're done
		if batchCount == 0 {
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

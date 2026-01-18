package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
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

// FetchStreamMessages retrieves messages from a stream using direct get with batched concurrent requests
func FetchStreamMessages(ctx context.Context, stream jetstream.Stream, batchSize, limit int, progress ProgressFunc) ([]MessageData, error) {
	info := stream.CachedInfo()
	streamName := info.Config.Name
	firstSeq := info.State.FirstSeq
	lastSeq := info.State.LastSeq

	if info.State.Msgs == 0 {
		return nil, nil
	}

	// Determine how many messages to fetch
	totalToFetch := int(lastSeq - firstSeq + 1)
	if limit > 0 && limit < totalToFetch {
		totalToFetch = limit
	}

	messages := make([]MessageData, 0, totalToFetch)
	var mu sync.Mutex
	var fetchedCount int64

	// Process in batches with concurrent requests within each batch
	currentSeq := firstSeq
	fetched := 0

	for fetched < totalToFetch && currentSeq <= lastSeq {
		// Determine batch size for this iteration
		batchEnd := currentSeq + uint64(batchSize)
		if batchEnd > lastSeq+1 {
			batchEnd = lastSeq + 1
		}

		// Fetch batch concurrently
		var wg sync.WaitGroup
		batchMessages := make([]MessageData, 0, batchSize)
		var batchMu sync.Mutex

		for seq := currentSeq; seq < batchEnd; seq++ {
			if fetched >= totalToFetch {
				break
			}

			wg.Add(1)
			go func(s uint64) {
				defer wg.Done()

				msg, err := stream.GetMsg(ctx, s)
				if err != nil {
					// Message might have been deleted or doesn't exist, skip it
					atomic.AddInt64(&fetchedCount, 1)
					if progress != nil {
						progress(int(atomic.LoadInt64(&fetchedCount)), totalToFetch)
					}
					return
				}

				batchMu.Lock()
				batchMessages = append(batchMessages, MessageData{
					StreamName: streamName,
					Sequence:   msg.Sequence,
					Timestamp:  msg.Time,
					Size:       len(msg.Data),
				})
				batchMu.Unlock()

				atomic.AddInt64(&fetchedCount, 1)
				if progress != nil {
					progress(int(atomic.LoadInt64(&fetchedCount)), totalToFetch)
				}
			}(seq)
		}

		wg.Wait()

		mu.Lock()
		messages = append(messages, batchMessages...)
		fetched = len(messages)
		mu.Unlock()

		currentSeq = batchEnd

		// Check if we've hit the limit
		if limit > 0 && fetched >= limit {
			break
		}
	}

	// Trim to limit if we exceeded it due to concurrent fetching
	if limit > 0 && len(messages) > limit {
		messages = messages[:limit]
	}

	return messages, nil
}

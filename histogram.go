package main

import (
	"math"
	"sort"
	"time"
)

// RateBucket represents a time bucket with message count and throughput
type RateBucket struct {
	Start      time.Time
	End        time.Time
	Count      int
	Bytes      int64
	Rate       float64 // messages per second
	Throughput float64 // bytes per second
}

// RateStatistics contains statistics for rate analysis
type RateStatistics struct {
	TotalMessages  int
	TotalBytes     int64
	StartTime      time.Time
	EndTime        time.Time
	TotalDuration  time.Duration
	AvgRate        float64
	P50Rate        float64
	P90Rate        float64
	P99Rate        float64
	P999Rate       float64
	MinRate        float64
	MaxRate        float64
	StdDevRate     float64
	AvgThroughput  float64 // bytes per second
	P50Throughput  float64
	P90Throughput  float64
	P99Throughput  float64
	P999Throughput float64
	MinThroughput  float64
	MaxThroughput  float64
	StdDevTput     float64
	AvgMsgSize     float64 // message size in bytes
	P50MsgSize     float64
	P90MsgSize     float64
	P99MsgSize     float64
	P999MsgSize    float64
	MinMsgSize     int
	MaxMsgSize     int
	StdDevMsgSize  float64
	FirstSeq       uint64  // first sequence number
	LastSeq        uint64  // last sequence number
	SeqRate        float64 // rate based on sequence numbers (msgs recorded/s)
	ActiveBuckets  int     // buckets with at least one message
	TotalBuckets   int
}

// RateHistogram represents message rates over time
type RateHistogram struct {
	Buckets     []RateBucket
	Granularity time.Duration
	Stats       RateStatistics
}

// StreamSummary holds summary info for a stream
type StreamSummary struct {
	Name     string
	Messages int
	Bytes    int64
	FirstSeq uint64
	LastSeq  uint64
	SeqRate  float64 // rate based on sequence numbers (msgs recorded/s)
}

// ReportSummary holds overall summary info
type ReportSummary struct {
	StartTime   time.Time
	EndTime     time.Time
	Duration    time.Duration
	StreamCount int
	TotalMsgs   int
	TotalBytes  int64
	TotalSeqs   uint64  // sum of (lastSeq - firstSeq) across all streams
	SeqRate     float64 // rate based on sequence numbers (msgs recorded/s)
	Streams     []StreamSummary
}

// BuildReportSummary creates a summary from collected messages
// Messages are expected to be sorted by timestamp
func BuildReportSummary(messages []MessageData, streamCount int) ReportSummary {
	if len(messages) == 0 {
		return ReportSummary{StreamCount: streamCount}
	}

	// Messages are sorted by timestamp, so first/last give us the time range
	summary := ReportSummary{
		StreamCount: streamCount,
		TotalMsgs:   len(messages),
		StartTime:   messages[0].Timestamp,
		EndTime:     messages[len(messages)-1].Timestamp,
	}
	summary.Duration = summary.EndTime.Sub(summary.StartTime)

	// Track per-stream stats with sequence tracking
	streamStats := make(map[string]*StreamSummary)

	for _, msg := range messages {
		summary.TotalBytes += int64(msg.Size)

		ss, ok := streamStats[msg.StreamName]
		if !ok {
			ss = &StreamSummary{
				Name:     msg.StreamName,
				FirstSeq: msg.Sequence,
				LastSeq:  msg.Sequence,
			}
			streamStats[msg.StreamName] = ss
		}
		ss.Messages++
		ss.Bytes += int64(msg.Size)

		// Track min/max sequence per stream
		if msg.Sequence < ss.FirstSeq {
			ss.FirstSeq = msg.Sequence
		}
		if msg.Sequence > ss.LastSeq {
			ss.LastSeq = msg.Sequence
		}
	}

	// Calculate sequence-based stats and convert map to slice
	for _, ss := range streamStats {
		// Calculate per-stream sequence rate
		if summary.Duration.Seconds() > 0 {
			seqCount := ss.LastSeq - ss.FirstSeq
			ss.SeqRate = float64(seqCount) / summary.Duration.Seconds()
		}

		// Accumulate total sequences across streams
		summary.TotalSeqs += ss.LastSeq - ss.FirstSeq

		summary.Streams = append(summary.Streams, *ss)
	}

	// Calculate overall sequence rate
	if summary.Duration.Seconds() > 0 {
		summary.SeqRate = float64(summary.TotalSeqs) / summary.Duration.Seconds()
	}

	// Sort by message count descending
	sort.Slice(summary.Streams, func(i, j int) bool {
		return summary.Streams[i].Messages > summary.Streams[j].Messages
	})

	return summary
}

// BuildRateHistogram creates a rate histogram from message data
func BuildRateHistogram(messages []MessageData, granularity time.Duration) *RateHistogram {
	if len(messages) == 0 {
		return &RateHistogram{Granularity: granularity}
	}

	// Find time range and sequence range
	minTime := messages[0].Timestamp
	maxTime := messages[len(messages)-1].Timestamp
	firstSeq := messages[0].Sequence
	lastSeq := messages[len(messages)-1].Sequence

	// Align to granularity boundaries
	startTime := minTime.Truncate(granularity)
	endTime := maxTime.Truncate(granularity).Add(granularity)

	// Create buckets
	numBuckets := int(endTime.Sub(startTime) / granularity)
	if numBuckets == 0 {
		numBuckets = 1
	}

	buckets := make([]RateBucket, numBuckets)
	for i := range buckets {
		buckets[i].Start = startTime.Add(time.Duration(i) * granularity)
		buckets[i].End = buckets[i].Start.Add(granularity)
	}

	// Count messages and bytes per bucket, collect message sizes
	var totalBytes int64
	msgSizes := make([]int, len(messages))
	for i, msg := range messages {
		bucketIdx := int(msg.Timestamp.Sub(startTime) / granularity)
		if bucketIdx >= len(buckets) {
			bucketIdx = len(buckets) - 1
		}
		if bucketIdx < 0 {
			bucketIdx = 0
		}
		buckets[bucketIdx].Count++
		buckets[bucketIdx].Bytes += int64(msg.Size)
		totalBytes += int64(msg.Size)
		msgSizes[i] = msg.Size
	}

	// Calculate rates and throughput
	granularitySecs := granularity.Seconds()
	for i := range buckets {
		buckets[i].Rate = float64(buckets[i].Count) / granularitySecs
		buckets[i].Throughput = float64(buckets[i].Bytes) / granularitySecs
	}

	hist := &RateHistogram{
		Buckets:     buckets,
		Granularity: granularity,
	}

	hist.Stats = calculateRateStats(buckets, len(messages), totalBytes, startTime, endTime, msgSizes, firstSeq, lastSeq)

	return hist
}

// calculateRateStats computes statistics from rate buckets and message sizes
func calculateRateStats(buckets []RateBucket, totalMessages int, totalBytes int64, startTime, endTime time.Time, msgSizes []int, firstSeq, lastSeq uint64) RateStatistics {
	if len(buckets) == 0 {
		return RateStatistics{}
	}

	stats := RateStatistics{
		TotalMessages: totalMessages,
		TotalBytes:    totalBytes,
		StartTime:     startTime,
		EndTime:       endTime,
		TotalDuration: endTime.Sub(startTime),
		TotalBuckets:  len(buckets),
		FirstSeq:      firstSeq,
		LastSeq:       lastSeq,
	}

	// Calculate sequence-based rate
	if stats.TotalDuration.Seconds() > 0 {
		seqCount := lastSeq - firstSeq
		stats.SeqRate = float64(seqCount) / stats.TotalDuration.Seconds()
	}

	// Collect rates and throughputs for percentile calculation
	rates := make([]float64, len(buckets))
	throughputs := make([]float64, len(buckets))
	var sumRate, sumTput float64
	stats.MinRate = buckets[0].Rate
	stats.MaxRate = buckets[0].Rate
	stats.MinThroughput = buckets[0].Throughput
	stats.MaxThroughput = buckets[0].Throughput

	for i, bucket := range buckets {
		rates[i] = bucket.Rate
		throughputs[i] = bucket.Throughput
		sumRate += bucket.Rate
		sumTput += bucket.Throughput

		if bucket.Rate < stats.MinRate {
			stats.MinRate = bucket.Rate
		}
		if bucket.Rate > stats.MaxRate {
			stats.MaxRate = bucket.Rate
		}
		if bucket.Throughput < stats.MinThroughput {
			stats.MinThroughput = bucket.Throughput
		}
		if bucket.Throughput > stats.MaxThroughput {
			stats.MaxThroughput = bucket.Throughput
		}
		if bucket.Count > 0 {
			stats.ActiveBuckets++
		}
	}

	// Average rate and throughput
	stats.AvgRate = sumRate / float64(len(buckets))
	stats.AvgThroughput = sumTput / float64(len(buckets))

	// Sort for percentiles
	sort.Float64s(rates)
	sort.Float64s(throughputs)

	// Calculate rate percentiles
	stats.P50Rate = percentileFloat64(rates, 0.50)
	stats.P90Rate = percentileFloat64(rates, 0.90)
	stats.P99Rate = percentileFloat64(rates, 0.99)
	stats.P999Rate = percentileFloat64(rates, 0.999)

	// Calculate throughput percentiles
	stats.P50Throughput = percentileFloat64(throughputs, 0.50)
	stats.P90Throughput = percentileFloat64(throughputs, 0.90)
	stats.P99Throughput = percentileFloat64(throughputs, 0.99)
	stats.P999Throughput = percentileFloat64(throughputs, 0.999)

	// Standard deviation for rate
	var sumSquaredDiff float64
	for _, rate := range rates {
		diff := rate - stats.AvgRate
		sumSquaredDiff += diff * diff
	}
	stats.StdDevRate = math.Sqrt(sumSquaredDiff / float64(len(rates)))

	// Standard deviation for throughput
	sumSquaredDiff = 0
	for _, tput := range throughputs {
		diff := tput - stats.AvgThroughput
		sumSquaredDiff += diff * diff
	}
	stats.StdDevTput = math.Sqrt(sumSquaredDiff / float64(len(throughputs)))

	// Calculate message size statistics
	if len(msgSizes) > 0 {
		// Convert to float64 for percentile calculation and find min/max
		sizesFloat := make([]float64, len(msgSizes))
		var sumSize float64
		stats.MinMsgSize = msgSizes[0]
		stats.MaxMsgSize = msgSizes[0]

		for i, size := range msgSizes {
			sizesFloat[i] = float64(size)
			sumSize += float64(size)
			if size < stats.MinMsgSize {
				stats.MinMsgSize = size
			}
			if size > stats.MaxMsgSize {
				stats.MaxMsgSize = size
			}
		}

		stats.AvgMsgSize = sumSize / float64(len(msgSizes))

		// Sort for percentiles
		sort.Float64s(sizesFloat)

		stats.P50MsgSize = percentileFloat64(sizesFloat, 0.50)
		stats.P90MsgSize = percentileFloat64(sizesFloat, 0.90)
		stats.P99MsgSize = percentileFloat64(sizesFloat, 0.99)
		stats.P999MsgSize = percentileFloat64(sizesFloat, 0.999)

		// Standard deviation for message size
		sumSquaredDiff = 0
		for _, size := range sizesFloat {
			diff := size - stats.AvgMsgSize
			sumSquaredDiff += diff * diff
		}
		stats.StdDevMsgSize = math.Sqrt(sumSquaredDiff / float64(len(sizesFloat)))
	}

	return stats
}

// percentileFloat64 calculates the p-th percentile from sorted values
func percentileFloat64(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}

	idx := p * float64(len(sorted)-1)
	lower := int(idx)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}

	weight := idx - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

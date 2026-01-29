package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"time"

	"js-traffic-history/web"
)

// GUIServer holds the state for the web-based GUI
type GUIServer struct {
	port        int
	openBrowser bool
	combined    *RateHistogram
	histograms  map[string]*RateHistogram
	summary     *ReportSummary
}

// JSONSummary is the JSON representation of ReportSummary
type JSONSummary struct {
	StartTime   time.Time           `json:"start_time"`
	EndTime     time.Time           `json:"end_time"`
	DurationNs  int64               `json:"duration_ns"`
	StreamCount int                 `json:"stream_count"`
	TotalMsgs   int                 `json:"total_msgs"`
	TotalBytes  int64               `json:"total_bytes"`
	TotalSeqs   uint64              `json:"total_seqs"`
	SeqRate     float64             `json:"seq_rate"`
	Streams     []JSONStreamSummary `json:"streams"`
}

// JSONStreamSummary is the JSON representation of StreamSummary
type JSONStreamSummary struct {
	Name     string  `json:"name"`
	Messages int     `json:"messages"`
	Bytes    int64   `json:"bytes"`
	FirstSeq uint64  `json:"first_seq"`
	LastSeq  uint64  `json:"last_seq"`
	SeqRate  float64 `json:"seq_rate"`
}

// JSONStreamBucketData is per-stream data within a bucket
type JSONStreamBucketData struct {
	Count    int   `json:"count"`
	SeqCount int   `json:"seq_count"`
	Bytes    int64 `json:"bytes"`
}

// JSONBucket is the JSON representation of RateBucket
type JSONBucket struct {
	Start      time.Time                        `json:"start"`
	End        time.Time                        `json:"end"`
	Count      int                              `json:"count"`
	SeqCount   int                              `json:"seq_count"`
	Bytes      int64                            `json:"bytes"`
	Rate       float64                          `json:"rate"`
	SeqRate    float64                          `json:"seq_rate"`
	Throughput float64                          `json:"throughput"`
	MinMsgSize int                              `json:"min_msg_size"`
	MaxMsgSize int                              `json:"max_msg_size"`
	SumMsgSize int64                            `json:"sum_msg_size"`
	PerStream  map[string]*JSONStreamBucketData `json:"per_stream,omitempty"`
}

// JSONStats is the JSON representation of RateStatistics
type JSONStats struct {
	TotalMessages    int       `json:"total_messages"`
	TotalBytes       int64     `json:"total_bytes"`
	StartTime        time.Time `json:"start_time"`
	EndTime          time.Time `json:"end_time"`
	TotalDurationNs  int64     `json:"total_duration_ns"`
	AvgRate          float64   `json:"avg_rate"`
	P50Rate          float64   `json:"p50_rate"`
	P90Rate          float64   `json:"p90_rate"`
	P99Rate          float64   `json:"p99_rate"`
	P999Rate         float64   `json:"p999_rate"`
	MinRate          float64   `json:"min_rate"`
	MaxRate          float64   `json:"max_rate"`
	StdDevRate       float64   `json:"stddev_rate"`
	AvgSeqRate       float64   `json:"avg_seq_rate"`
	P50SeqRate       float64   `json:"p50_seq_rate"`
	P90SeqRate       float64   `json:"p90_seq_rate"`
	P99SeqRate       float64   `json:"p99_seq_rate"`
	P999SeqRate      float64   `json:"p999_seq_rate"`
	MinSeqRate       float64   `json:"min_seq_rate"`
	MaxSeqRate       float64   `json:"max_seq_rate"`
	StdDevSeqRate    float64   `json:"stddev_seq_rate"`
	AvgThroughput    float64   `json:"avg_throughput"`
	P50Throughput    float64   `json:"p50_throughput"`
	P90Throughput    float64   `json:"p90_throughput"`
	P99Throughput    float64   `json:"p99_throughput"`
	P999Throughput   float64   `json:"p999_throughput"`
	MinThroughput    float64   `json:"min_throughput"`
	MaxThroughput    float64   `json:"max_throughput"`
	StdDevThroughput float64   `json:"stddev_throughput"`
	AvgMsgSize       float64   `json:"avg_msg_size"`
	P50MsgSize       float64   `json:"p50_msg_size"`
	P90MsgSize       float64   `json:"p90_msg_size"`
	P99MsgSize       float64   `json:"p99_msg_size"`
	P999MsgSize      float64   `json:"p999_msg_size"`
	MinMsgSize       int       `json:"min_msg_size"`
	MaxMsgSize       int       `json:"max_msg_size"`
	StdDevMsgSize    float64   `json:"stddev_msg_size"`
	FirstSeq         uint64    `json:"first_seq"`
	LastSeq          uint64    `json:"last_seq"`
	SeqRate          float64   `json:"overall_seq_rate"`
	ActiveBuckets    int       `json:"active_buckets"`
	TotalBuckets     int       `json:"total_buckets"`
}

// JSONHistogram is the JSON representation of RateHistogram
type JSONHistogram struct {
	Buckets       []JSONBucket `json:"buckets"`
	GranularityNs int64        `json:"granularity_ns"`
	Stats         JSONStats    `json:"stats"`
}

// NewGUIServer creates a new GUI server
func NewGUIServer(port int, autoBrowser bool, combined *RateHistogram, histograms map[string]*RateHistogram, summary *ReportSummary) *GUIServer {
	return &GUIServer{
		port:        port,
		openBrowser: autoBrowser,
		combined:    combined,
		histograms:  histograms,
		summary:     summary,
	}
}

// convertSummary converts ReportSummary to JSONSummary
func convertSummary(s *ReportSummary) JSONSummary {
	if s == nil {
		return JSONSummary{}
	}

	streams := make([]JSONStreamSummary, len(s.Streams))
	for i, st := range s.Streams {
		streams[i] = JSONStreamSummary{
			Name:     st.Name,
			Messages: st.Messages,
			Bytes:    st.Bytes,
			FirstSeq: st.FirstSeq,
			LastSeq:  st.LastSeq,
			SeqRate:  st.SeqRate,
		}
	}

	return JSONSummary{
		StartTime:   s.StartTime,
		EndTime:     s.EndTime,
		DurationNs:  s.Duration.Nanoseconds(),
		StreamCount: s.StreamCount,
		TotalMsgs:   s.TotalMsgs,
		TotalBytes:  s.TotalBytes,
		TotalSeqs:   s.TotalSeqs,
		SeqRate:     s.SeqRate,
		Streams:     streams,
	}
}

// convertHistogram converts RateHistogram to JSONHistogram
func convertHistogram(h *RateHistogram) JSONHistogram {
	if h == nil {
		return JSONHistogram{}
	}

	buckets := make([]JSONBucket, len(h.Buckets))
	for i, b := range h.Buckets {
		buckets[i] = JSONBucket{
			Start:      b.Start,
			End:        b.End,
			Count:      b.Count,
			SeqCount:   b.SeqCount,
			Bytes:      b.Bytes,
			Rate:       b.Rate,
			SeqRate:    b.SeqRate,
			Throughput: b.Throughput,
			MinMsgSize: b.MinMsgSize,
			MaxMsgSize: b.MaxMsgSize,
			SumMsgSize: b.SumMsgSize,
		}
		// Include per-stream data if available
		if len(b.PerStream) > 0 {
			buckets[i].PerStream = make(map[string]*JSONStreamBucketData, len(b.PerStream))
			for name, data := range b.PerStream {
				buckets[i].PerStream[name] = &JSONStreamBucketData{
					Count:    data.Count,
					SeqCount: data.SeqCount,
					Bytes:    data.Bytes,
				}
			}
		}
	}

	stats := JSONStats{
		TotalMessages:    h.Stats.TotalMessages,
		TotalBytes:       h.Stats.TotalBytes,
		StartTime:        h.Stats.StartTime,
		EndTime:          h.Stats.EndTime,
		TotalDurationNs:  h.Stats.TotalDuration.Nanoseconds(),
		AvgRate:          h.Stats.AvgRate,
		P50Rate:          h.Stats.P50Rate,
		P90Rate:          h.Stats.P90Rate,
		P99Rate:          h.Stats.P99Rate,
		P999Rate:         h.Stats.P999Rate,
		MinRate:          h.Stats.MinRate,
		MaxRate:          h.Stats.MaxRate,
		StdDevRate:       h.Stats.StdDevRate,
		AvgSeqRate:       h.Stats.AvgSeqRate,
		P50SeqRate:       h.Stats.P50SeqRate,
		P90SeqRate:       h.Stats.P90SeqRate,
		P99SeqRate:       h.Stats.P99SeqRate,
		P999SeqRate:      h.Stats.P999SeqRate,
		MinSeqRate:       h.Stats.MinSeqRate,
		MaxSeqRate:       h.Stats.MaxSeqRate,
		StdDevSeqRate:    h.Stats.StdDevSeqRate,
		AvgThroughput:    h.Stats.AvgThroughput,
		P50Throughput:    h.Stats.P50Throughput,
		P90Throughput:    h.Stats.P90Throughput,
		P99Throughput:    h.Stats.P99Throughput,
		P999Throughput:   h.Stats.P999Throughput,
		MinThroughput:    h.Stats.MinThroughput,
		MaxThroughput:    h.Stats.MaxThroughput,
		StdDevThroughput: h.Stats.StdDevTput,
		AvgMsgSize:       h.Stats.AvgMsgSize,
		P50MsgSize:       h.Stats.P50MsgSize,
		P90MsgSize:       h.Stats.P90MsgSize,
		P99MsgSize:       h.Stats.P99MsgSize,
		P999MsgSize:      h.Stats.P999MsgSize,
		MinMsgSize:       h.Stats.MinMsgSize,
		MaxMsgSize:       h.Stats.MaxMsgSize,
		StdDevMsgSize:    h.Stats.StdDevMsgSize,
		FirstSeq:         h.Stats.FirstSeq,
		LastSeq:          h.Stats.LastSeq,
		SeqRate:          h.Stats.SeqRate,
		ActiveBuckets:    h.Stats.ActiveBuckets,
		TotalBuckets:     h.Stats.TotalBuckets,
	}

	return JSONHistogram{
		Buckets:       buckets,
		GranularityNs: h.Granularity.Nanoseconds(),
		Stats:         stats,
	}
}

// handleIndex serves the main HTML page
func (g *GUIServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	// Serve index.html from embedded filesystem
	staticFS, err := fs.Sub(web.StaticFS, "static")
	if err != nil {
		http.Error(w, "Failed to access static files", http.StatusInternalServerError)
		return
	}

	content, err := fs.ReadFile(staticFS, "index.html")
	if err != nil {
		http.Error(w, "Failed to read index.html", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(content)
}

// handleSummary returns the report summary as JSON
func (g *GUIServer) handleSummary(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(convertSummary(g.summary))
}

// maxGUIBuckets is the maximum number of buckets to send to the GUI
const maxGUIBuckets = 3000

// downsampleHistogram reduces the number of buckets.
// If useAverage is false (default), it takes the MAX rate from each group of buckets
// to preserve peaks in the graph.
// If useAverage is true, it calculates the average rate over the aggregated time span.
// Counts and bytes are always summed for accurate totals in tooltips.
// Statistics are preserved from the original histogram.
func downsampleHistogram(hist *RateHistogram, maxBuckets int, useAverage bool) *RateHistogram {
	if hist == nil || len(hist.Buckets) <= maxBuckets {
		return hist
	}

	buckets := hist.Buckets
	factor := (len(buckets) + maxBuckets - 1) / maxBuckets // ceiling division

	newBuckets := make([]RateBucket, 0, maxBuckets)
	for i := 0; i < len(buckets); i += factor {
		end := i + factor
		if end > len(buckets) {
			end = len(buckets)
		}

		// Aggregate buckets
		agg := RateBucket{
			Start: buckets[i].Start,
			End:   buckets[end-1].End,
		}

		for j := i; j < end; j++ {
			// Sum counts and bytes for totals (used in tooltips)
			agg.Count += buckets[j].Count
			agg.SeqCount += buckets[j].SeqCount
			agg.Bytes += buckets[j].Bytes

			if !useAverage {
				// Use MAX for rates to preserve peaks in the graph
				if buckets[j].Rate > agg.Rate {
					agg.Rate = buckets[j].Rate
				}
				if buckets[j].SeqRate > agg.SeqRate {
					agg.SeqRate = buckets[j].SeqRate
				}
				if buckets[j].Throughput > agg.Throughput {
					agg.Throughput = buckets[j].Throughput
				}
			}

			// Merge per-stream data (sum for distribution)
			if len(buckets[j].PerStream) > 0 {
				if agg.PerStream == nil {
					agg.PerStream = make(map[string]*StreamBucketData)
				}
				for name, data := range buckets[j].PerStream {
					if agg.PerStream[name] == nil {
						agg.PerStream[name] = &StreamBucketData{}
					}
					agg.PerStream[name].Count += data.Count
					agg.PerStream[name].SeqCount += data.SeqCount
					agg.PerStream[name].Bytes += data.Bytes
				}
			}
		}

		// Calculate average rates if useAverage is true
		if useAverage {
			duration := agg.End.Sub(agg.Start).Seconds()
			if duration > 0 {
				agg.Rate = float64(agg.Count) / duration
				agg.SeqRate = float64(agg.SeqCount) / duration
				agg.Throughput = float64(agg.Bytes) / duration
			}
		}

		newBuckets = append(newBuckets, agg)
	}

	return &RateHistogram{
		Buckets:     newBuckets,
		Granularity: hist.Granularity * time.Duration(factor),
		Stats:       hist.Stats, // Keep original stats for accurate statistics
	}
}

// filterHistogramByTime returns buckets within the given time range
// and recalculates statistics for the filtered range
func filterHistogramByTime(hist *RateHistogram, startTime, endTime *time.Time) *RateHistogram {
	if hist == nil || (startTime == nil && endTime == nil) {
		return hist
	}

	var filtered []RateBucket
	for _, b := range hist.Buckets {
		if startTime != nil && b.End.Before(*startTime) {
			continue
		}
		if endTime != nil && b.Start.After(*endTime) {
			continue
		}
		filtered = append(filtered, b)
	}

	if len(filtered) == 0 {
		return &RateHistogram{
			Buckets:     filtered,
			Granularity: hist.Granularity,
			Stats:       RateStatistics{},
		}
	}

	// Recalculate statistics for the filtered buckets
	stats := CalculateStatsFromBuckets(filtered)

	return &RateHistogram{
		Buckets:     filtered,
		Granularity: hist.Granularity,
		Stats:       stats,
	}
}

// extractStreamHistogram creates a histogram for a specific stream from the combined histogram's per-stream data
func (g *GUIServer) extractStreamHistogram(streamName string) *RateHistogram {
	if g.combined == nil {
		return nil
	}

	buckets := make([]RateBucket, len(g.combined.Buckets))
	granularitySecs := g.combined.Granularity.Seconds()

	for i, b := range g.combined.Buckets {
		buckets[i] = RateBucket{
			Start: b.Start,
			End:   b.End,
		}
		if streamData, ok := b.PerStream[streamName]; ok {
			buckets[i].Count = streamData.Count
			buckets[i].SeqCount = streamData.SeqCount
			buckets[i].Bytes = streamData.Bytes
			buckets[i].Rate = float64(streamData.Count) / granularitySecs
			buckets[i].SeqRate = float64(streamData.SeqCount) / granularitySecs
			buckets[i].Throughput = float64(streamData.Bytes) / granularitySecs
		}
	}

	// Calculate stats from the extracted buckets
	stats := CalculateStatsFromBuckets(buckets)

	return &RateHistogram{
		Buckets:     buckets,
		Granularity: g.combined.Granularity,
		Stats:       stats,
	}
}

// handleHistogram returns histogram data as JSON
func (g *GUIServer) handleHistogram(w http.ResponseWriter, r *http.Request) {
	streamName := r.URL.Query().Get("stream")
	startParam := r.URL.Query().Get("start")
	endParam := r.URL.Query().Get("end")
	downsampleParam := r.URL.Query().Get("downsample")
	useAverageDownsample := downsampleParam == "avg"

	var hist *RateHistogram
	if streamName == "" {
		hist = g.combined
	} else {
		// Try to get from pre-built histograms, or extract from combined
		if g.histograms != nil {
			var ok bool
			hist, ok = g.histograms[streamName]
			if !ok {
				http.Error(w, "Stream not found", http.StatusNotFound)
				return
			}
		} else {
			// Extract from combined histogram's per-stream data
			hist = g.extractStreamHistogram(streamName)
			if hist == nil {
				http.Error(w, "Stream not found", http.StatusNotFound)
				return
			}
		}
	}

	// Parse time range parameters (unix timestamps in seconds)
	var startTime, endTime *time.Time
	if startParam != "" {
		if ts, err := strconv.ParseFloat(startParam, 64); err == nil {
			t := time.Unix(int64(ts), int64((ts-float64(int64(ts)))*1e9))
			startTime = &t
		}
	}
	if endParam != "" {
		if ts, err := strconv.ParseFloat(endParam, 64); err == nil {
			t := time.Unix(int64(ts), int64((ts-float64(int64(ts)))*1e9))
			endTime = &t
		}
	}

	// Filter by time range if specified
	if startTime != nil || endTime != nil {
		hist = filterHistogramByTime(hist, startTime, endTime)
	}

	// Downsample only if too many buckets
	if hist != nil && len(hist.Buckets) > maxGUIBuckets {
		hist = downsampleHistogram(hist, maxGUIBuckets, useAverageDownsample)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(convertHistogram(hist))
}

// handleStreams returns the list of stream names
func (g *GUIServer) handleStreams(w http.ResponseWriter, r *http.Request) {
	var streams []string
	if g.histograms != nil {
		streams = make([]string, 0, len(g.histograms))
		for name := range g.histograms {
			streams = append(streams, name)
		}
	} else if g.summary != nil {
		// Get stream names from summary
		streams = make([]string, 0, len(g.summary.Streams))
		for _, s := range g.summary.Streams {
			streams = append(streams, s.Name)
		}
	}
	sort.Strings(streams)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(streams)
}

// handleDistribution returns per-stream distribution, optionally filtered by time range
func (g *GUIServer) handleDistribution(w http.ResponseWriter, r *http.Request) {
	startParam := r.URL.Query().Get("start")
	endParam := r.URL.Query().Get("end")

	// Parse time range parameters (unix timestamps in seconds)
	var startTime, endTime *time.Time
	if startParam != "" {
		if ts, err := strconv.ParseFloat(startParam, 64); err == nil {
			t := time.Unix(int64(ts), int64((ts-float64(int64(ts)))*1e9))
			startTime = &t
		}
	}
	if endParam != "" {
		if ts, err := strconv.ParseFloat(endParam, 64); err == nil {
			t := time.Unix(int64(ts), int64((ts-float64(int64(ts)))*1e9))
			endTime = &t
		}
	}

	// If no time range specified, return the original summary streams
	if startTime == nil && endTime == nil {
		if g.summary != nil {
			streams := make([]JSONStreamSummary, len(g.summary.Streams))
			for i, s := range g.summary.Streams {
				streams[i] = JSONStreamSummary{
					Name:     s.Name,
					Messages: s.Messages,
					Bytes:    s.Bytes,
					FirstSeq: s.FirstSeq,
					LastSeq:  s.LastSeq,
					SeqRate:  s.SeqRate,
				}
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(streams)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]JSONStreamSummary{})
		return
	}

	// Calculate per-stream stats for the filtered time range using combined histogram's per-stream data
	streamData := make(map[string]*JSONStreamSummary)

	if g.combined != nil {
		for _, b := range g.combined.Buckets {
			// Check if bucket overlaps with the time range
			if startTime != nil && b.End.Before(*startTime) {
				continue
			}
			if endTime != nil && b.Start.After(*endTime) {
				continue
			}
			// Aggregate per-stream data from this bucket
			for name, data := range b.PerStream {
				if streamData[name] == nil {
					streamData[name] = &JSONStreamSummary{Name: name}
				}
				streamData[name].Messages += data.Count
				streamData[name].Bytes += data.Bytes
			}
		}
	}

	// Convert to slice and filter out empty streams
	streams := make([]JSONStreamSummary, 0, len(streamData))
	for _, s := range streamData {
		if s.Messages > 0 {
			streams = append(streams, *s)
		}
	}

	// Sort by message count descending
	sort.Slice(streams, func(i, j int) bool {
		return streams[i].Messages > streams[j].Messages
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(streams)
}

// openBrowser opens the default browser to the specified URL
func openBrowser(url string) error {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}

	return cmd.Start()
}

// Start starts the GUI server
func (g *GUIServer) Start() error {
	// Create a sub-filesystem for static files
	staticFS, err := fs.Sub(web.StaticFS, "static")
	if err != nil {
		return fmt.Errorf("failed to create static filesystem: %w", err)
	}

	// Set up routes
	mux := http.NewServeMux()
	mux.HandleFunc("/", g.handleIndex)
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))
	mux.HandleFunc("/api/summary", g.handleSummary)
	mux.HandleFunc("/api/histogram", g.handleHistogram)
	mux.HandleFunc("/api/streams", g.handleStreams)
	mux.HandleFunc("/api/distribution", g.handleDistribution)

	addr := fmt.Sprintf(":%d", g.port)
	url := fmt.Sprintf("http://localhost:%d", g.port)

	fmt.Println()
	fmt.Printf("==> GUI server ready at %s\n", url)
	fmt.Println("==> Press Ctrl+C to stop")
	fmt.Println()

	// Open browser after a short delay if enabled
	if g.openBrowser {
		go func() {
			time.Sleep(500 * time.Millisecond)
			if err := openBrowser(url); err != nil {
				fmt.Printf("Could not open browser automatically. Please open %s manually.\n", url)
			}
		}()
	}

	return http.ListenAndServe(addr, mux)
}

// StartGUIServer creates and starts the GUI server
func StartGUIServer(port int, autoBrowser bool, combined *RateHistogram, histograms map[string]*RateHistogram, summary *ReportSummary) error {
	server := NewGUIServer(port, autoBrowser, combined, histograms, summary)
	return server.Start()
}

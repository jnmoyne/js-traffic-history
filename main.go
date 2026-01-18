package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/choria-io/fisk"
)

var (
	version = "0.1.0"
)

type Config struct {
	Context         string
	RateGranularity time.Duration
	ShowGraph       bool
	ShowRate        bool
	ShowThroughput  bool
	StreamFilter    string
	BatchSize       int
	Limit           int
	PerStream       bool
	CSVFile         string
	MinRatePct      float64
	StartTime       string
	EndTime         string
	Since           time.Duration
}

func main() {
	cfg := parseFlags()

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() Config {
	cfg := Config{}

	app := fisk.New("js-traffic-history", "Analyze message rates across NATS JetStream streams with limits retention policy")
	app.Version(version)
	app.Author("Synadia")

	app.Flag("context", "NATS context name (uses default if empty)").
		Short('c').
		StringVar(&cfg.Context)

	app.Flag("granularity", "Time bucket size for rate calculation").
		Default("1s").
		DurationVar(&cfg.RateGranularity)

	app.Flag("graph", "Display ASCII graph").
		Short('g').
		BoolVar(&cfg.ShowGraph)

	app.Flag("rate", "Show message rate graph and stats").
		Default("true").
		BoolVar(&cfg.ShowRate)

	app.Flag("throughput", "Show throughput graph and stats").
		Default("true").
		BoolVar(&cfg.ShowThroughput)

	app.Flag("stream", "Filter to specific stream").
		Short('s').
		StringVar(&cfg.StreamFilter)

	app.Flag("batch-size", "Messages per batch request").
		Default("10000").
		IntVar(&cfg.BatchSize)

	app.Flag("limit", "Max messages to analyze per stream (0 = all)").
		Short('l').
		Default("0").
		IntVar(&cfg.Limit)

	app.Flag("per-stream", "Also show stats and graphs for each individual stream").
		UnNegatableBoolVar(&cfg.PerStream)

	app.Flag("csv", "Export histogram data to CSV file").
		StringVar(&cfg.CSVFile)

	app.Flag("min-rate-pct", "Skip graph buckets below this percentage of max rate").
		Default("10").
		Float64Var(&cfg.MinRatePct)

	app.Flag("start", "Start timestamp (RFC3339 or 2006-01-02 15:04:05)").
		StringVar(&cfg.StartTime)

	app.Flag("end", "End timestamp (RFC3339 or 2006-01-02 15:04:05)").
		StringVar(&cfg.EndTime)

	app.Flag("since", "Relative start time (e.g., 1h, 30m, 2h30m)").
		DurationVar(&cfg.Since)

	app.MustParseWithUsage(os.Args[1:])

	if cfg.RateGranularity <= 0 {
		fisk.Fatalf("--granularity must be positive")
	}

	if cfg.BatchSize <= 0 {
		fisk.Fatalf("--batch-size must be positive")
	}

	// Add .csv extension if missing
	if cfg.CSVFile != "" && !strings.HasSuffix(strings.ToLower(cfg.CSVFile), ".csv") {
		cfg.CSVFile += ".csv"
	}

	return cfg
}

// parseTimestamp parses a timestamp string in various formats
func parseTimestamp(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse timestamp %q (use RFC3339 or 2006-01-02 15:04:05 format)", s)
}

func run(cfg Config) error {
	ctx := context.Background()

	// Parse time filters
	var startTime, endTime *time.Time
	if cfg.Since > 0 {
		if cfg.StartTime != "" {
			return fmt.Errorf("cannot use both --since and --start")
		}
		t := time.Now().Add(-cfg.Since)
		startTime = &t
	} else if cfg.StartTime != "" {
		t, err := parseTimestamp(cfg.StartTime)
		if err != nil {
			return err
		}
		startTime = &t
	}
	if cfg.EndTime != "" {
		t, err := parseTimestamp(cfg.EndTime)
		if err != nil {
			return err
		}
		endTime = &t
	}

	// Show time filter info
	if startTime != nil || endTime != nil {
		fmt.Print("Time filter: ")
		if startTime != nil {
			fmt.Printf("from %s ", startTime.Format("2006-01-02 15:04:05"))
		}
		if endTime != nil {
			fmt.Printf("to %s", endTime.Format("2006-01-02 15:04:05"))
		}
		fmt.Println()
	}

	// Connect to NATS
	fmt.Println("Connecting to NATS...")
	nc, js, err := ConnectNATS(cfg.Context)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	// Get streams with limits retention
	fmt.Println("Discovering streams with limits retention policy...")
	streams, err := GetLimitsStreams(ctx, js, cfg.StreamFilter)
	if err != nil {
		return fmt.Errorf("failed to get streams: %w", err)
	}

	if len(streams) == 0 {
		fmt.Println("No streams with limits retention policy found.")
		return nil
	}

	fmt.Printf("Found %d stream(s) to analyze\n\n", len(streams))

	// Collect all messages for combined analysis
	var allMessages []MessageData

	// First pass: fetch all messages from all streams
	streamMessages := make(map[string][]MessageData)
	for _, stream := range streams {
		streamName := stream.CachedInfo().Config.Name
		msgCount := stream.CachedInfo().State.Msgs

		fmt.Printf("Fetching messages from stream: %s (%d messages)\n", streamName, msgCount)

		messages, err := FetchStreamMessages(ctx, js, stream, cfg.BatchSize, cfg.Limit, startTime, endTime, PrintProgress)
		ClearProgress()
		if err != nil {
			fmt.Printf("Warning: failed to fetch messages from %s: %v\n", streamName, err)
			continue
		}

		if len(messages) == 0 {
			if startTime != nil || endTime != nil {
				fmt.Printf("Stream %s has no messages in the specified time range\n", streamName)
			} else {
				fmt.Printf("Stream %s has no messages to analyze\n\n", streamName)
			}
			continue
		}

		// Sort messages by timestamp for proper analysis
		sort.Slice(messages, func(i, j int) bool {
			return messages[i].Timestamp.Before(messages[j].Timestamp)
		})

		streamMessages[streamName] = messages
		allMessages = append(allMessages, messages...)
	}

	fmt.Println()

	// Sort all messages by timestamp for combined analysis
	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Timestamp.Before(allMessages[j].Timestamp)
	})

	// Print report summary at the start
	summary := BuildReportSummary(allMessages, len(streams))
	PrintReportSummary(summary)

	// Build graph options
	graphOpts := GraphOptions{
		ShowGraph:      cfg.ShowGraph,
		ShowRate:       cfg.ShowRate,
		ShowThroughput: cfg.ShowThroughput,
		MinRatePct:     cfg.MinRatePct,
	}

	// Always show combined analysis
	if len(allMessages) > 0 {
		PrintCombinedHeader(len(streams), len(allMessages))

		// Build and display combined rate over time
		rateHist := BuildRateHistogram(allMessages, cfg.RateGranularity)
		PrintRateHistogram(rateHist, graphOpts)

		// Export to CSV if requested
		if cfg.CSVFile != "" && !cfg.PerStream {
			if err := WriteCSV(cfg.CSVFile, rateHist, "combined"); err != nil {
				return fmt.Errorf("failed to write CSV: %w", err)
			}
			fmt.Printf("CSV data exported to %s\n", cfg.CSVFile)
		}
	}

	// Show per-stream analysis if requested
	if cfg.PerStream {
		csvFirstWrite := true
		for _, stream := range streams {
			streamName := stream.CachedInfo().Config.Name
			messages, ok := streamMessages[streamName]
			if !ok || len(messages) == 0 {
				continue
			}

			PrintStreamHeader(streamName, len(messages))

			// Build and display rate over time
			rateHist := BuildRateHistogram(messages, cfg.RateGranularity)
			PrintRateHistogram(rateHist, graphOpts)

			// Write per-stream data to CSV if requested
			if cfg.CSVFile != "" {
				if csvFirstWrite {
					if err := WriteCSV(cfg.CSVFile, rateHist, streamName); err != nil {
						return fmt.Errorf("failed to write CSV: %w", err)
					}
					csvFirstWrite = false
				} else {
					if err := AppendCSV(cfg.CSVFile, rateHist, streamName); err != nil {
						return fmt.Errorf("failed to append to CSV: %w", err)
					}
				}
			}

			fmt.Println()
		}
		if cfg.CSVFile != "" && !csvFirstWrite {
			fmt.Printf("CSV data exported to %s\n", cfg.CSVFile)
		}
	}

	return nil
}

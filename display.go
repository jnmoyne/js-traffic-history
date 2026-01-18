package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	graphWidth       = 40
	headerWidth      = 70
	progressBarWidth = 30
)

// PrintProgress prints a progress bar
func PrintProgress(current, total int) {
	if total == 0 {
		return
	}
	pct := float64(current) / float64(total)
	filled := int(pct * float64(progressBarWidth))
	if filled > progressBarWidth {
		filled = progressBarWidth
	}
	empty := progressBarWidth - filled
	fmt.Printf("\r  [%s%s] %d/%d (%.0f%%)",
		strings.Repeat("█", filled),
		strings.Repeat("░", empty),
		current, total, pct*100)
}

// ClearProgress clears the progress bar line
func ClearProgress() {
	fmt.Printf("\r%s\r", strings.Repeat(" ", 60))
}

// PrintReportSummary prints the overall summary at the start of the report
func PrintReportSummary(summary ReportSummary) {
	fmt.Println(strings.Repeat("=", headerWidth))
	fmt.Println("TRAFFIC HISTORY REPORT")
	fmt.Println(strings.Repeat("=", headerWidth))
	fmt.Println()

	if summary.TotalMsgs == 0 {
		fmt.Println("  No messages found")
		fmt.Println()
		return
	}

	fmt.Println("Overview:")
	fmt.Printf("  Start Time:       %s\n", summary.StartTime.Format("2006-01-02 15:04:05.000"))
	fmt.Printf("  End Time:         %s\n", summary.EndTime.Format("2006-01-02 15:04:05.000"))
	fmt.Printf("  Duration:         %s\n", formatDuration(summary.Duration))
	fmt.Printf("  Streams:          %d\n", summary.StreamCount)
	fmt.Printf("  Total Messages:   %d\n", summary.TotalMsgs)
	fmt.Printf("  Total Data:       %s\n", formatBytes(summary.TotalBytes))
	if summary.Duration.Seconds() > 0 {
		fmt.Printf("  Avg Throughput:   %s/s\n", formatBytes(int64(float64(summary.TotalBytes)/summary.Duration.Seconds())))
		fmt.Printf("  Avg Rate:         %.2f msg/s\n", float64(summary.TotalMsgs)/summary.Duration.Seconds())
	}
	fmt.Println()

	// Print stream breakdown
	if len(summary.Streams) > 0 {
		fmt.Println("Streams by Message Count:")
		maxMsgs := summary.Streams[0].Messages
		for _, s := range summary.Streams {
			barLen := int((float64(s.Messages) / float64(maxMsgs)) * float64(graphWidth))
			if barLen < 1 && s.Messages > 0 {
				barLen = 1
			}
			bar := strings.Repeat("█", barLen)
			fmt.Printf("  %-20s %8d msgs %10s | %s\n", s.Name, s.Messages, formatBytes(s.Bytes), bar)
		}
		fmt.Println()
	}
}

// PrintStreamHeader prints a header for a single stream's analysis
func PrintStreamHeader(streamName string, msgCount int) {
	fmt.Println(strings.Repeat("-", headerWidth))
	fmt.Printf("Stream: %s (%d messages)\n", streamName, msgCount)
	fmt.Println(strings.Repeat("-", headerWidth))
	fmt.Println()
}

// PrintCombinedHeader prints a header for combined analysis
func PrintCombinedHeader(streamCount, msgCount int) {
	fmt.Println(strings.Repeat("-", headerWidth))
	fmt.Printf("COMBINED (%d streams, %d total messages)\n", streamCount, msgCount)
	fmt.Println(strings.Repeat("-", headerWidth))
	fmt.Println()
}

// GraphOptions controls which graphs and stats to display
type GraphOptions struct {
	ShowGraph      bool
	ShowRate       bool
	ShowThroughput bool
	MinRatePct     float64 // Skip buckets below this percentage of max rate
}

// PrintRateHistogram displays the rate over time and statistics
func PrintRateHistogram(hist *RateHistogram, opts GraphOptions) {
	fmt.Printf("-- Message Rate Over Time (granularity: %s) %s\n", formatDuration(hist.Granularity), strings.Repeat("-", 22))
	fmt.Println()

	if len(hist.Buckets) == 0 {
		fmt.Println("  No data to display")
		fmt.Println()
		return
	}

	if opts.ShowGraph {
		if opts.ShowRate {
			printRateGraph(hist, opts.MinRatePct)
		}
		if opts.ShowThroughput {
			printThroughputGraph(hist, opts.MinRatePct)
		}
	}

	printRateStats(hist.Stats, opts.ShowRate, opts.ShowThroughput)
}

// printRateGraph prints a time-series graph showing rate per bucket over time
func printRateGraph(hist *RateHistogram, minRatePct float64) {
	if len(hist.Buckets) == 0 {
		return
	}

	// Find max rate for scaling
	maxRate := 0.0
	for _, bucket := range hist.Buckets {
		if bucket.Rate > maxRate {
			maxRate = bucket.Rate
		}
	}

	if maxRate == 0 {
		fmt.Println("  No messages in any bucket")
		fmt.Println()
		return
	}

	// Calculate threshold
	threshold := maxRate * minRatePct / 100.0

	fmt.Printf("  Message Rate (scale: 0 to %.1f msg/s, hiding < %.1f%%)\n", maxRate, minRatePct)
	fmt.Printf("  %-20s | %12s | %s\n", "Time", "Rate", "Graph")
	fmt.Printf("  %s-+-%s-+-%s\n", strings.Repeat("-", 20), strings.Repeat("-", 12), strings.Repeat("-", graphWidth))

	// Track skipped bucket ranges
	var skipStart *time.Time
	var skipEnd time.Time
	skipCount := 0

	printSkipped := func() {
		if skipCount > 0 && skipStart != nil {
			startStr := skipStart.Format("2006-01-02 15:04:05")
			endStr := skipEnd.Format("2006-01-02 15:04:05")
			fmt.Printf("  %-20s | %12s | ... %d buckets below threshold ...\n", startStr, "->"+endStr, skipCount)
			skipCount = 0
			skipStart = nil
		}
	}

	for _, bucket := range hist.Buckets {
		if bucket.Rate < threshold {
			if skipStart == nil {
				t := bucket.Start
				skipStart = &t
			}
			skipEnd = bucket.Start
			skipCount++
			continue
		}

		// Print any accumulated skipped range
		printSkipped()

		barLen := int((bucket.Rate / maxRate) * float64(graphWidth))
		if barLen < 0 {
			barLen = 0
		}
		bar := strings.Repeat("█", barLen)
		timeStr := bucket.Start.Format("2006-01-02 15:04:05")
		fmt.Printf("  %-20s | %12.2f | %s\n", timeStr, bucket.Rate, bar)
	}

	// Print any remaining skipped range at the end
	printSkipped()

	fmt.Println()
}

// printThroughputGraph prints a time-series graph showing throughput per bucket
func printThroughputGraph(hist *RateHistogram, minRatePct float64) {
	if len(hist.Buckets) == 0 {
		return
	}

	// Find max throughput for scaling
	maxTput := 0.0
	for _, bucket := range hist.Buckets {
		if bucket.Throughput > maxTput {
			maxTput = bucket.Throughput
		}
	}

	if maxTput == 0 {
		return
	}

	// Calculate threshold
	threshold := maxTput * minRatePct / 100.0

	fmt.Printf("  Throughput (scale: 0 to %s/s, hiding < %.1f%%)\n", formatBytes(int64(maxTput)), minRatePct)
	fmt.Printf("  %-20s | %12s | %s\n", "Time", "Throughput", "Graph")
	fmt.Printf("  %s-+-%s-+-%s\n", strings.Repeat("-", 20), strings.Repeat("-", 12), strings.Repeat("-", graphWidth))

	// Track skipped bucket ranges
	var skipStart *time.Time
	var skipEnd time.Time
	skipCount := 0

	printSkipped := func() {
		if skipCount > 0 && skipStart != nil {
			startStr := skipStart.Format("2006-01-02 15:04:05")
			endStr := skipEnd.Format("2006-01-02 15:04:05")
			fmt.Printf("  %-20s | %12s | ... %d buckets below threshold ...\n", startStr, "->"+endStr, skipCount)
			skipCount = 0
			skipStart = nil
		}
	}

	for _, bucket := range hist.Buckets {
		if bucket.Throughput < threshold {
			if skipStart == nil {
				t := bucket.Start
				skipStart = &t
			}
			skipEnd = bucket.Start
			skipCount++
			continue
		}

		// Print any accumulated skipped range
		printSkipped()

		barLen := int((bucket.Throughput / maxTput) * float64(graphWidth))
		if barLen < 0 {
			barLen = 0
		}
		bar := strings.Repeat("█", barLen)
		timeStr := bucket.Start.Format("2006-01-02 15:04:05")
		fmt.Printf("  %-20s | %12s | %s\n", timeStr, formatBytesPerSec(bucket.Throughput), bar)
	}

	// Print any remaining skipped range at the end
	printSkipped()

	fmt.Println()
}

// printRateStats prints the rate and throughput statistics
func printRateStats(stats RateStatistics, showRate, showThroughput bool) {
	fmt.Println("Statistics:")
	fmt.Printf("  Total Messages:   %d\n", stats.TotalMessages)
	fmt.Printf("  Total Data:       %s\n", formatBytes(stats.TotalBytes))
	fmt.Printf("  Time Span:        %s\n", formatDuration(stats.TotalDuration))
	fmt.Printf("  Total Buckets:    %d (active: %d, %.1f%%)\n",
		stats.TotalBuckets, stats.ActiveBuckets,
		float64(stats.ActiveBuckets)/float64(stats.TotalBuckets)*100)
	fmt.Println()

	if showRate {
		fmt.Println("  Message Rate:")
		fmt.Printf("    Average:        %.2f msg/s\n", stats.AvgRate)
		fmt.Printf("    P50:            %.2f msg/s\n", stats.P50Rate)
		fmt.Printf("    P90:            %.2f msg/s\n", stats.P90Rate)
		fmt.Printf("    P99:            %.2f msg/s\n", stats.P99Rate)
		fmt.Printf("    P99.9:          %.2f msg/s\n", stats.P999Rate)
		fmt.Printf("    Min:            %.2f msg/s\n", stats.MinRate)
		fmt.Printf("    Max:            %.2f msg/s\n", stats.MaxRate)
		fmt.Printf("    Std Dev:        %.2f msg/s\n", stats.StdDevRate)
		fmt.Println()
	}

	if showThroughput {
		fmt.Println("  Throughput:")
		fmt.Printf("    Average:        %s/s\n", formatBytes(int64(stats.AvgThroughput)))
		fmt.Printf("    P50:            %s/s\n", formatBytes(int64(stats.P50Throughput)))
		fmt.Printf("    P90:            %s/s\n", formatBytes(int64(stats.P90Throughput)))
		fmt.Printf("    P99:            %s/s\n", formatBytes(int64(stats.P99Throughput)))
		fmt.Printf("    P99.9:          %s/s\n", formatBytes(int64(stats.P999Throughput)))
		fmt.Printf("    Min:            %s/s\n", formatBytes(int64(stats.MinThroughput)))
		fmt.Printf("    Max:            %s/s\n", formatBytes(int64(stats.MaxThroughput)))
		fmt.Printf("    Std Dev:        %s/s\n", formatBytes(int64(stats.StdDevTput)))
		fmt.Println()
	}
}

// formatDuration formats a duration in a human-readable way
func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}

	if d < time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		secs := d.Seconds() - float64(mins*60)
		return fmt.Sprintf("%dm%.1fs", mins, secs)
	}

	hours := int(d.Hours())
	mins := int(d.Minutes()) - hours*60
	secs := d.Seconds() - float64(hours*3600) - float64(mins*60)
	return fmt.Sprintf("%dh%dm%.1fs", hours, mins, secs)
}

// formatBytes formats bytes in human-readable form
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// formatBytesPerSec formats bytes/sec in human-readable form
func formatBytesPerSec(b float64) string {
	return formatBytes(int64(b)) + "/s"
}

// WriteCSV exports histogram data to a CSV file
func WriteCSV(filename string, hist *RateHistogram, streamName string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"stream", "timestamp", "count", "bytes", "rate_msg_per_sec", "throughput_bytes_per_sec"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data rows
	for _, bucket := range hist.Buckets {
		row := []string{
			streamName,
			bucket.Start.Format(time.RFC3339),
			fmt.Sprintf("%d", bucket.Count),
			fmt.Sprintf("%d", bucket.Bytes),
			fmt.Sprintf("%.2f", bucket.Rate),
			fmt.Sprintf("%.2f", bucket.Throughput),
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	return nil
}

// AppendCSV appends histogram data to an existing CSV file
func AppendCSV(filename string, hist *RateHistogram, streamName string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open CSV file for append: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write data rows (no header for append)
	for _, bucket := range hist.Buckets {
		row := []string{
			streamName,
			bucket.Start.Format(time.RFC3339),
			fmt.Sprintf("%d", bucket.Count),
			fmt.Sprintf("%d", bucket.Bytes),
			fmt.Sprintf("%.2f", bucket.Rate),
			fmt.Sprintf("%.2f", bucket.Throughput),
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	return nil
}

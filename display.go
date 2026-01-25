package main

import (
	"cmp"
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"golang.org/x/term"
)

const (
	headerWidth      = 70
	progressBarWidth = 30
	// Fixed column widths for rate graph: "  " + time(19) + " | "
	rateGraphFixedCols = 2 + 19 + 3
)

// buildLabeledRateBar creates a bar with rates embedded: stored rate in █ section, deleted rate in ░ section, total rate right-aligned
func buildLabeledRateBar(graphWidth, storedBarLen, deletedBarLen int, storedRate, deletedRate, totalRate float64) string {
	totalBarLen := storedBarLen + deletedBarLen

	// Create arrays to track bar type at each position: 0=empty, 1=stored, 2=deleted
	barType := make([]int, graphWidth)
	barChars := make([]rune, graphWidth)
	hasLabel := make([]bool, graphWidth) // Track positions with label text
	for i := range barChars {
		barChars[i] = ' '
		barType[i] = 0
		hasLabel[i] = false
	}

	// Fill with bar characters
	for i := 0; i < storedBarLen && i < graphWidth; i++ {
		barChars[i] = '█'
		barType[i] = 1
	}
	for i := storedBarLen; i < totalBarLen && i < graphWidth; i++ {
		barChars[i] = '░'
		barType[i] = 2
	}

	// Format the rates
	storedStr := formatScaleValue(storedRate)
	deletedStr := formatScaleValue(deletedRate)
	totalStr := formatScaleValue(totalRate)

	// Place stored rate left-aligned in the black section (only on black bar positions)
	if storedRate > 0 {
		for i, r := range storedStr {
			if i < graphWidth && barType[i] == 1 { // Only place on black bar
				barChars[i] = r
				hasLabel[i] = true
			}
		}
	}

	// Place deleted rate left-aligned in the grey section (only on grey bar positions)
	if deletedRate > 0 {
		for i, r := range deletedStr {
			pos := storedBarLen + i
			if pos < graphWidth && barType[pos] == 2 { // Only place on grey bar
				barChars[pos] = r
				hasLabel[pos] = true
			}
		}
	}

	// Place total rate right-aligned at the end of the graph
	totalPos := graphWidth - len(totalStr)
	if totalPos < 0 {
		totalPos = 0
	}
	for i, r := range totalStr {
		if totalPos+i < graphWidth {
			barChars[totalPos+i] = r
			hasLabel[totalPos+i] = true
		}
	}

	// Build final string with ANSI codes for text on bar
	// barType 1 (stored/black): use inverse video
	// barType 2 (deleted/grey): use white text on grey background
	var result strings.Builder
	currentStyle := 0 // 0=normal, 1=inverse (for black), 2=white-on-grey (for deleted)
	for i := 0; i < graphWidth; i++ {
		targetStyle := 0
		if hasLabel[i] {
			if barType[i] == 1 {
				targetStyle = 1 // inverse for black bar
			} else if barType[i] == 2 {
				targetStyle = 2 // white on grey for deleted bar
			}
		}

		if targetStyle != currentStyle {
			if currentStyle != 0 {
				result.WriteString("\033[0m") // Reset first
			}
			if targetStyle == 1 {
				result.WriteString("\033[7m") // Inverse
			} else if targetStyle == 2 {
				result.WriteString("\033[30;107m") // Black text on bright white background
			}
			currentStyle = targetStyle
		}
		result.WriteRune(barChars[i])
	}
	if currentStyle != 0 {
		result.WriteString("\033[0m") // Reset at end
	}

	return result.String()
}

// buildLabeledTputBar creates a throughput bar with value right-aligned
func buildLabeledTputBar(graphWidth, barLen int, throughput float64) string {
	// Track if position is on bar
	isBar := make([]bool, graphWidth)
	barChars := make([]rune, graphWidth)
	for i := range barChars {
		barChars[i] = ' '
		isBar[i] = false
	}

	// Fill with bar characters
	for i := 0; i < barLen && i < graphWidth; i++ {
		barChars[i] = '▓'
		isBar[i] = true
	}

	// Format throughput and place right-aligned
	tputStr := formatBytes(int64(throughput)) + "/s"
	tputPos := graphWidth - len(tputStr)
	if tputPos < 0 {
		tputPos = 0
	}
	for i, r := range tputStr {
		if tputPos+i < graphWidth {
			barChars[tputPos+i] = r
		}
	}

	// Build final string with ANSI codes for inverted text on bar
	var result strings.Builder
	inInverse := false
	for i := 0; i < graphWidth; i++ {
		isOnBar := isBar[i] && i >= tputPos && i < tputPos+len(tputStr)
		if isOnBar && !inInverse {
			result.WriteString("\033[7m") // Start inverse
			inInverse = true
		} else if !isOnBar && inInverse {
			result.WriteString("\033[0m") // End inverse
			inInverse = false
		}
		result.WriteRune(barChars[i])
	}
	if inInverse {
		result.WriteString("\033[0m") // Reset at end
	}

	return result.String()
}

// printGraphScale prints a scale line for a graph with tick marks at 0%, 50%, and 100%
func printGraphScale(prefix string, graphWidth int, maxValue float64, unit string) {
	// Calculate positions for 0%, 50%, 100%
	pos50 := graphWidth / 2
	pos100 := graphWidth

	// Format values
	val0 := "0"
	val50 := formatScaleValue(maxValue * 0.5)
	val100 := formatScaleValue(maxValue)

	// Build the scale line
	scaleLine := make([]byte, graphWidth)
	for i := range scaleLine {
		scaleLine[i] = ' '
	}

	// Place tick marks
	scaleLine[0] = '|'
	if pos50 > 0 && pos50 < graphWidth {
		scaleLine[pos50] = '|'
	}
	if pos100 > 0 && pos100 <= graphWidth {
		scaleLine[pos100-1] = '|'
	}

	// Print scale line with labels below
	fmt.Printf("%s%s\n", prefix, string(scaleLine))

	// Build label line
	labelLine := make([]byte, graphWidth)
	for i := range labelLine {
		labelLine[i] = ' '
	}

	// Place labels (0 at start, 50% in middle, 100% at end)
	copy(labelLine[0:], val0)
	mid50 := pos50 - len(val50)/2
	if mid50 < len(val0)+1 {
		mid50 = len(val0) + 1
	}
	if mid50+len(val50) <= graphWidth {
		copy(labelLine[mid50:], val50)
	}

	// 100% label with unit at the end
	endLabel := val100 + " " + unit
	endPos := graphWidth - len(endLabel)
	if endPos > mid50+len(val50)+1 {
		copy(labelLine[endPos:], endLabel)
	}

	fmt.Printf("%s%s\n", prefix, string(labelLine))
}

// formatScaleValue formats a value for scale display
func formatScaleValue(v float64) string {
	if v >= 1000000 {
		return fmt.Sprintf("%.1fM", v/1000000)
	}
	if v >= 1000 {
		return fmt.Sprintf("%.1fK", v/1000)
	}
	if v >= 100 {
		return fmt.Sprintf("%.0f", v)
	}
	if v >= 10 {
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.2f", v)
}

// getTerminalWidth returns the terminal width, or a default if it can't be determined
func getTerminalWidth() int {
	width, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || width <= 0 {
		return 120 // default width
	}
	return width
}

// getGraphWidth calculates the available width for the graph based on terminal size
func getGraphWidth(fixedCols int) int {
	termWidth := getTerminalWidth()
	graphWidth := termWidth - fixedCols
	if graphWidth < 20 {
		graphWidth = 20 // minimum graph width
	}
	return graphWidth
}

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
func PrintReportSummary(summary ReportSummary, stats *RateStatistics, distribution bool) {
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
	fmt.Printf("  Duration:                      %s (%s to %s)\n",
		formatDuration(summary.Duration),
		summary.StartTime.Format("2006-01-02 15:04:05"),
		summary.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("  Streams:                       %d\n", summary.StreamCount)
	fmt.Printf("  Total Messages:                %d\n", summary.TotalMsgs)

	if stats != nil {
		fmt.Printf("  Total Messages (per seq nums):  %s\n", humanize.Comma(int64(stats.LastSeq-stats.FirstSeq+1)))
	}

	fmt.Printf("  Total Data:                    %s\n", formatBytes(summary.TotalBytes))
	if summary.Duration.Seconds() > 0 {
		fmt.Printf("  Avg Throughput:                %s/s\n", formatBytes(int64(float64(summary.TotalBytes)/summary.Duration.Seconds())))
	}
	fmt.Println()

	// Print detailed stats
	if stats != nil {
		fmt.Println("  Message Rate (by stored msgs):")
		fmt.Printf("    Average:                     %.2f msg/s\n", stats.AvgRate)
		fmt.Printf("    P50:                         %.2f msg/s\n", stats.P50Rate)
		fmt.Printf("    P90:                         %.2f msg/s\n", stats.P90Rate)
		fmt.Printf("    P99:                         %.2f msg/s\n", stats.P99Rate)
		fmt.Printf("    P99.9:                       %.2f msg/s\n", stats.P999Rate)
		fmt.Printf("    Min:                         %.2f msg/s\n", stats.MinRate)
		fmt.Printf("    Max:                         %.2f msg/s\n", stats.MaxRate)
		fmt.Printf("    Std Dev:                     %.2f msg/s\n", stats.StdDevRate)
		fmt.Println()

		fmt.Println("  Message Rate (per sequence numbers with deletes interpolated):")
		fmt.Printf("    Average:                     %.2f msg/s\n", stats.AvgSeqRate)
		fmt.Printf("    P50:                         %.2f msg/s\n", stats.P50SeqRate)
		fmt.Printf("    P90:                         %.2f msg/s\n", stats.P90SeqRate)
		fmt.Printf("    P99:                         %.2f msg/s\n", stats.P99SeqRate)
		fmt.Printf("    P99.9:                       %.2f msg/s\n", stats.P999SeqRate)
		fmt.Printf("    Min:                         %.2f msg/s\n", stats.MinSeqRate)
		fmt.Printf("    Max:                         %.2f msg/s\n", stats.MaxSeqRate)
		fmt.Printf("    Std Dev:                     %.2f msg/s\n", stats.StdDevSeqRate)
		fmt.Println()

		fmt.Println("  Throughput:")
		fmt.Printf("    Average:                     %s/s\n", formatBytes(int64(stats.AvgThroughput)))
		fmt.Printf("    P50:                         %s/s\n", formatBytes(int64(stats.P50Throughput)))
		fmt.Printf("    P90:                         %s/s\n", formatBytes(int64(stats.P90Throughput)))
		fmt.Printf("    P99:                         %s/s\n", formatBytes(int64(stats.P99Throughput)))
		fmt.Printf("    P99.9:                       %s/s\n", formatBytes(int64(stats.P999Throughput)))
		fmt.Printf("    Min:                         %s/s\n", formatBytes(int64(stats.MinThroughput)))
		fmt.Printf("    Max:                         %s/s\n", formatBytes(int64(stats.MaxThroughput)))
		fmt.Printf("    Std Dev:                     %s/s\n", formatBytes(int64(stats.StdDevTput)))
		fmt.Println()

		fmt.Println("  Message Size:")
		fmt.Printf("    Average:                     %s\n", formatBytes(int64(stats.AvgMsgSize)))
		fmt.Printf("    P50:                         %s\n", formatBytes(int64(stats.P50MsgSize)))
		fmt.Printf("    P90:                         %s\n", formatBytes(int64(stats.P90MsgSize)))
		fmt.Printf("    P99:                         %s\n", formatBytes(int64(stats.P99MsgSize)))
		fmt.Printf("    P99.9:                       %s\n", formatBytes(int64(stats.P999MsgSize)))
		fmt.Printf("    Min:                         %s\n", formatBytes(int64(stats.MinMsgSize)))
		fmt.Printf("    Max:                         %s\n", formatBytes(int64(stats.MaxMsgSize)))
		fmt.Printf("    Std Dev:                     %s\n", formatBytes(int64(stats.StdDevMsgSize)))
		fmt.Println()
	}

	// Print stream breakdown as aligned table
	if len(summary.Streams) > 0 && distribution {
		// Find max stream name length for alignment
		maxNameLen := 6 // minimum "Stream" header width
		for _, s := range summary.Streams {
			if len(s.Name) > maxNameLen {
				maxNameLen = len(s.Name)
			}
		}

		// Calculate graph width for stream tables
		// Fixed cols: "  " + name(maxNameLen) + " | " + messages(10) + " | " + data(10) + " | "
		streamGraphWidth := getGraphWidth(2 + maxNameLen + 3 + 10 + 3 + 10 + 3)

		// Table 1: Streams by Stored Message Count
		fmt.Println("Streams Distribution by Stored Message Count:")
		fmt.Printf("  %-*s | %10s | %10s | %s\n", maxNameLen, "Stream", "Messages", "Data", "Graph")
		fmt.Printf("  %s-+-%s-+-%s-+-%s\n",
			strings.Repeat("-", maxNameLen),
			strings.Repeat("-", 10),
			strings.Repeat("-", 10),
			strings.Repeat("-", streamGraphWidth))

		maxMsgs := summary.Streams[0].Messages
		for _, s := range summary.Streams {
			barLen := int((float64(s.Messages) / float64(maxMsgs)) * float64(streamGraphWidth))
			if barLen < 1 && s.Messages > 0 {
				barLen = 1
			}
			bar := strings.Repeat("█", barLen)
			fmt.Printf("  %-*s | %10d | %10s | %s\n", maxNameLen, s.Name, s.Messages, formatBytes(s.Bytes), bar)
		}
		fmt.Println()

		// Table 2: Streams by Sequence Number Count
		// Sort streams by sequence count for this table
		streamsBySeq := make([]StreamSummary, len(summary.Streams))
		copy(streamsBySeq, summary.Streams)
		slices.SortFunc(streamsBySeq, func(a, b StreamSummary) int {
			seqCountA := a.LastSeq - a.FirstSeq
			seqCountB := b.LastSeq - b.FirstSeq
			return cmp.Compare(seqCountB, seqCountA) // descending order
		})

		// Recalculate for table 2 which has different fixed columns
		// Fixed cols: "  " + name(maxNameLen) + " | " + seqCount(10) + " | " + avgRate(12) + " | "
		streamGraphWidth2 := getGraphWidth(2 + maxNameLen + 3 + 10 + 3 + 12 + 3)

		fmt.Println("Streams Distribution by per Sequence Number Count:")
		fmt.Printf("  %-*s | %10s | %12s | %s\n", maxNameLen, "Stream", "Seq Count", "Avg Rate", "Graph")
		fmt.Printf("  %s-+-%s-+-%s-+-%s\n",
			strings.Repeat("-", maxNameLen),
			strings.Repeat("-", 10),
			strings.Repeat("-", 12),
			strings.Repeat("-", streamGraphWidth2))

		maxSeqCount := streamsBySeq[0].LastSeq - streamsBySeq[0].FirstSeq + 1
		for _, s := range streamsBySeq {
			seqCount := s.LastSeq - s.FirstSeq
			barLen := int((float64(seqCount) / float64(maxSeqCount)) * float64(streamGraphWidth2))
			if barLen < 1 && seqCount > 0 {
				barLen = 1
			}
			bar := strings.Repeat("█", barLen)
			fmt.Printf("  %-*s | %10d | %9.2f/s | %s\n", maxNameLen, s.Name, seqCount, s.SeqRate, bar)
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

// GraphOptions controls which graphs and stats to display
type GraphOptions struct {
	ShowGraph      bool
	ShowRate       bool
	ShowThroughput bool
	MinRatePct     float64 // Skip buckets below this percentage of max rate
}

// PrintRateHistogram displays the rate over time and statistics
func PrintRateHistogram(hist *RateHistogram, opts GraphOptions) {
	fmt.Printf("-- Stored Message Rate Over Time (granularity: %s) %s\n", formatDuration(hist.Granularity), strings.Repeat("-", 22))
	fmt.Println()

	if len(hist.Buckets) == 0 {
		fmt.Println("  No data to display")
		fmt.Println()
		return
	}

	if opts.ShowGraph {
		if opts.ShowRate && opts.ShowThroughput {
			printCombinedGraph(hist, opts.MinRatePct)
		} else if opts.ShowRate {
			printRateGraph(hist, opts.MinRatePct)
		} else if opts.ShowThroughput {
			printThroughputGraph(hist, opts.MinRatePct)
		}
	}

	printRateStats(hist.Stats, opts.ShowRate, opts.ShowThroughput)
}

// printRateGraph prints a time-series graph showing rate per bucket over time
// Shows stored messages (█) and interpolated deletes (░) in different shades
func printRateGraph(hist *RateHistogram, minRatePct float64) {
	if len(hist.Buckets) == 0 {
		return
	}

	// Calculate graph width based on terminal size
	graphWidth := getGraphWidth(rateGraphFixedCols)

	// Find max SeqRate for scaling (includes both stored and deleted)
	maxSeqRate := 0.0
	for _, bucket := range hist.Buckets {
		if bucket.SeqRate > maxSeqRate {
			maxSeqRate = bucket.SeqRate
		}
	}

	if maxSeqRate == 0 {
		fmt.Println("  No messages in any bucket")
		fmt.Println()
		return
	}

	// Calculate threshold based on SeqRate
	threshold := maxSeqRate * minRatePct / 100.0

	fmt.Printf("  Message Rate (hiding < %.1f%%, █=stored ░=deleted, total right-aligned)\n", minRatePct)
	fmt.Printf("  %-19s | %s\n", "Time", "Graph (stored | deleted | total)")
	fmt.Printf("  %s-+-%s\n", strings.Repeat("-", 19), strings.Repeat("-", graphWidth))

	// Print scale line at top
	scalePrefix := fmt.Sprintf("  %-19s | ", "")
	printGraphScale(scalePrefix, graphWidth, maxSeqRate, "msg/s")

	// Track skipped bucket ranges (empty buckets)
	var skipStart *time.Time
	var skipEnd time.Time
	skipCount := 0

	// Track deleted-only bucket ranges (buckets with only deleted messages, no stored)
	var delOnlyStart *time.Time
	var delOnlyEnd time.Time
	delOnlyCount := 0
	delOnlyTotal := 0

	printSkipped := func() {
		if skipCount > 0 && skipStart != nil {
			startStr := skipStart.Format("2006-01-02 15:04:05")
			duration := skipEnd.Sub(*skipStart) + hist.Granularity
			durationStr := "+" + formatDuration(duration)
			rateMsg := fmt.Sprintf("... %d skipped %s ...", skipCount, durationStr)
			fmt.Printf("  %-19s | %-*s\n", startStr, graphWidth, rateMsg)
			skipCount = 0
			skipStart = nil
		}
	}

	printDelOnly := func() {
		if delOnlyCount > 0 && delOnlyStart != nil {
			startStr := delOnlyStart.Format("2006-01-02 15:04:05")
			duration := delOnlyEnd.Sub(*delOnlyStart) + hist.Granularity
			durationStr := "+" + formatDuration(duration)
			rateMsg := fmt.Sprintf("... %d del-only %s ...", delOnlyCount, durationStr)
			fmt.Printf("  %-19s | %-*s\n", startStr, graphWidth, rateMsg)
			delOnlyCount = 0
			delOnlyTotal = 0
			delOnlyStart = nil
		}
	}

	for _, bucket := range hist.Buckets {
		// Always hide empty buckets, or those below threshold
		if bucket.SeqCount == 0 || bucket.SeqRate < threshold {
			// First flush any deleted-only range
			printDelOnly()
			if skipStart == nil {
				t := bucket.Start
				skipStart = &t
			}
			skipEnd = bucket.Start
			skipCount++
			continue
		}

		// Check if this bucket has only deleted messages (no stored)
		if bucket.Count == 0 && bucket.SeqCount > 0 {
			// First flush any skipped range
			printSkipped()
			if delOnlyStart == nil {
				t := bucket.Start
				delOnlyStart = &t
			}
			delOnlyEnd = bucket.Start
			delOnlyCount++
			delOnlyTotal += bucket.SeqCount
			continue
		}

		// Print any accumulated ranges
		printSkipped()
		printDelOnly()

		// Calculate bar lengths for stored and deleted portions
		totalBarLen := int((bucket.SeqRate / maxSeqRate) * float64(graphWidth))
		storedBarLen := int((bucket.Rate / maxSeqRate) * float64(graphWidth))
		if totalBarLen < 0 {
			totalBarLen = 0
		}
		if storedBarLen < 0 {
			storedBarLen = 0
		}
		if storedBarLen > totalBarLen {
			storedBarLen = totalBarLen
		}
		deletedBarLen := totalBarLen - storedBarLen

		// Calculate deleted rate
		deletedRate := bucket.SeqRate - bucket.Rate

		// Build labeled bar with rates embedded
		bar := buildLabeledRateBar(graphWidth, storedBarLen, deletedBarLen, bucket.Rate, deletedRate, bucket.SeqRate)

		timeStr := bucket.Start.Format("2006-01-02 15:04:05")
		fmt.Printf("  %-19s | %s\n", timeStr, bar)
	}

	// Print any remaining ranges at the end
	printSkipped()
	printDelOnly()

	// Print scale line at bottom
	printGraphScale(scalePrefix, graphWidth, maxSeqRate, "msg/s")

	fmt.Println()
}

// printCombinedGraph prints rate and throughput on the same line
func printCombinedGraph(hist *RateHistogram, minRatePct float64) {
	if len(hist.Buckets) == 0 {
		return
	}

	// Calculate graph widths based on terminal size
	// Fixed cols: "  " + time(19) + " | " + rateGraph + " | " + tputGraph
	// Fixed parts: 2 + 19 + 3 + 3 = 27, plus two graph columns
	termWidth := getTerminalWidth()
	availableForGraphs := termWidth - 27
	if availableForGraphs < 20 {
		availableForGraphs = 20
	}
	// Split available space between rate and throughput graphs (60/40 split favoring rate)
	rateGraphWidth := availableForGraphs * 6 / 10
	tputGraphWidth := availableForGraphs - rateGraphWidth
	if rateGraphWidth < 10 {
		rateGraphWidth = 10
	}
	if tputGraphWidth < 10 {
		tputGraphWidth = 10
	}

	// Find max SeqRate and max Throughput for scaling
	maxSeqRate := 0.0
	maxTput := 0.0
	for _, bucket := range hist.Buckets {
		if bucket.SeqRate > maxSeqRate {
			maxSeqRate = bucket.SeqRate
		}
		if bucket.Throughput > maxTput {
			maxTput = bucket.Throughput
		}
	}

	if maxSeqRate == 0 && maxTput == 0 {
		fmt.Println("  No messages in any bucket")
		fmt.Println()
		return
	}

	// Calculate thresholds
	rateThreshold := maxSeqRate * minRatePct / 100.0
	tputThreshold := maxTput * minRatePct / 100.0

	fmt.Printf("  Rate (█=stored ░=deleted) | Throughput (▓) | hiding < %.1f%%\n", minRatePct)
	fmt.Printf("  %-19s | %-*s | %s\n",
		"Time", rateGraphWidth, "Rate Graph", "Tput Graph")
	fmt.Printf("  %s-+-%s-+-%s\n",
		strings.Repeat("-", 19),
		strings.Repeat("-", rateGraphWidth),
		strings.Repeat("-", tputGraphWidth))

	// Print scale lines at top
	printCombinedGraphScale(rateGraphWidth, tputGraphWidth, maxSeqRate, maxTput)

	// Track skipped bucket ranges (empty buckets)
	var skipStart *time.Time
	var skipEnd time.Time
	skipCount := 0

	// Track deleted-only bucket ranges (buckets with only deleted messages, no stored)
	var delOnlyStart *time.Time
	var delOnlyEnd time.Time
	delOnlyCount := 0
	delOnlyTotal := 0

	printSkipped := func() {
		if skipCount > 0 && skipStart != nil {
			startStr := skipStart.Format("2006-01-02 15:04:05")
			duration := skipEnd.Sub(*skipStart) + hist.Granularity
			durationStr := "+" + formatDuration(duration)
			rateMsg := fmt.Sprintf("... %d skipped %s ...", skipCount, durationStr)
			tputMsg := fmt.Sprintf("... %s ...", durationStr)
			fmt.Printf("  %-19s | %-*s | %-*s\n",
				startStr, rateGraphWidth, rateMsg, tputGraphWidth, tputMsg)
			skipCount = 0
			skipStart = nil
		}
	}

	printDelOnly := func() {
		if delOnlyCount > 0 && delOnlyStart != nil {
			startStr := delOnlyStart.Format("2006-01-02 15:04:05")
			duration := delOnlyEnd.Sub(*delOnlyStart) + hist.Granularity
			durationStr := "+" + formatDuration(duration)
			rateMsg := fmt.Sprintf("... %d del-only %s ...", delOnlyCount, durationStr)
			tputMsg := fmt.Sprintf("... %s ...", durationStr)
			fmt.Printf("  %-19s | %-*s | %-*s\n",
				startStr, rateGraphWidth, rateMsg, tputGraphWidth, tputMsg)
			delOnlyCount = 0
			delOnlyTotal = 0
			delOnlyStart = nil
		}
	}

	for _, bucket := range hist.Buckets {
		// Always hide empty buckets, or those below threshold
		if (bucket.SeqCount == 0 && bucket.Bytes == 0) ||
			(bucket.SeqRate < rateThreshold && bucket.Throughput < tputThreshold) {
			// First flush any deleted-only range
			printDelOnly()
			if skipStart == nil {
				t := bucket.Start
				skipStart = &t
			}
			skipEnd = bucket.Start
			skipCount++
			continue
		}

		// Check if this bucket has only deleted messages (no stored)
		if bucket.Count == 0 && bucket.SeqCount > 0 {
			// First flush any skipped range
			printSkipped()
			if delOnlyStart == nil {
				t := bucket.Start
				delOnlyStart = &t
			}
			delOnlyEnd = bucket.Start
			delOnlyCount++
			delOnlyTotal += bucket.SeqCount
			continue
		}

		// Print any accumulated ranges
		printSkipped()
		printDelOnly()

		// Calculate rate bar lengths for stored and deleted portions
		var storedBarLen, deletedBarLen int
		if maxSeqRate > 0 {
			totalBarLen := int((bucket.SeqRate / maxSeqRate) * float64(rateGraphWidth))
			storedBarLen = int((bucket.Rate / maxSeqRate) * float64(rateGraphWidth))
			if totalBarLen < 0 {
				totalBarLen = 0
			}
			if storedBarLen < 0 {
				storedBarLen = 0
			}
			if storedBarLen > totalBarLen {
				storedBarLen = totalBarLen
			}
			deletedBarLen = totalBarLen - storedBarLen
		}

		// Calculate deleted rate
		deletedRate := bucket.SeqRate - bucket.Rate

		// Build labeled rate bar with rates embedded
		rateBar := buildLabeledRateBar(rateGraphWidth, storedBarLen, deletedBarLen, bucket.Rate, deletedRate, bucket.SeqRate)

		// Calculate throughput bar with label
		var tputBarLen int
		if maxTput > 0 {
			tputBarLen = int((bucket.Throughput / maxTput) * float64(tputGraphWidth))
			if tputBarLen < 0 {
				tputBarLen = 0
			}
		}
		tputBar := buildLabeledTputBar(tputGraphWidth, tputBarLen, bucket.Throughput)

		timeStr := bucket.Start.Format("2006-01-02 15:04:05")
		fmt.Printf("  %-19s | %s | %s\n",
			timeStr, rateBar, tputBar)
	}

	// Print any remaining ranges at the end
	printSkipped()
	printDelOnly()

	// Print scale lines for both graphs
	printCombinedGraphScale(rateGraphWidth, tputGraphWidth, maxSeqRate, maxTput)

	fmt.Println()
}

// printCombinedGraphScale prints scale lines for the combined rate and throughput graphs
func printCombinedGraphScale(rateWidth, tputWidth int, maxRate, maxTput float64) {
	prefix := fmt.Sprintf("  %-19s | ", "")

	// Build rate scale tick marks
	rateScale := make([]byte, rateWidth)
	for i := range rateScale {
		rateScale[i] = ' '
	}
	rateScale[0] = '|'
	if rateWidth/2 > 0 {
		rateScale[rateWidth/2] = '|'
	}
	rateScale[rateWidth-1] = '|'

	// Build tput scale tick marks
	tputScale := make([]byte, tputWidth)
	for i := range tputScale {
		tputScale[i] = ' '
	}
	tputScale[0] = '|'
	if tputWidth/2 > 0 {
		tputScale[tputWidth/2] = '|'
	}
	if tputWidth > 0 {
		tputScale[tputWidth-1] = '|'
	}

	fmt.Printf("%s%s | %s\n", prefix, string(rateScale), string(tputScale))

	// Build rate labels
	rateLabels := make([]byte, rateWidth)
	for i := range rateLabels {
		rateLabels[i] = ' '
	}
	copy(rateLabels[0:], "0")
	rateMid := formatScaleValue(maxRate * 0.5)
	midPos := rateWidth/2 - len(rateMid)/2
	if midPos > 1 {
		copy(rateLabels[midPos:], rateMid)
	}
	rateEnd := formatScaleValue(maxRate) + " msg/s"
	endPos := rateWidth - len(rateEnd)
	if endPos > midPos+len(rateMid)+1 {
		copy(rateLabels[endPos:], rateEnd)
	}

	// Build tput labels
	tputLabels := make([]byte, tputWidth)
	for i := range tputLabels {
		tputLabels[i] = ' '
	}
	copy(tputLabels[0:], "0")
	tputEnd := formatBytes(int64(maxTput)) + "/s"
	tputEndPos := tputWidth - len(tputEnd)
	if tputEndPos > 1 {
		copy(tputLabels[tputEndPos:], tputEnd)
	}

	fmt.Printf("%s%s | %s\n", prefix, string(rateLabels), string(tputLabels))
}

// printThroughputGraph prints a time-series graph showing throughput per bucket
func printThroughputGraph(hist *RateHistogram, minRatePct float64) {
	if len(hist.Buckets) == 0 {
		return
	}

	// Calculate graph width based on terminal size
	// Fixed cols: "  " + time(20) + " | " + throughput(12) + " | "
	graphWidth := getGraphWidth(2 + 20 + 3 + 12 + 3)

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

	fmt.Printf("  Throughput (hiding < %.1f%%)\n", minRatePct)
	fmt.Printf("  %-20s | %12s | %s\n", "Time", "Throughput", "Graph")
	fmt.Printf("  %s-+-%s-+-%s\n", strings.Repeat("-", 20), strings.Repeat("-", 12), strings.Repeat("-", graphWidth))

	// Print scale line at top
	scalePrefix := fmt.Sprintf("  %-20s | %12s | ", "", "")
	printGraphScale(scalePrefix, graphWidth, maxTput, "B/s")

	// Track skipped bucket ranges
	var skipStart *time.Time
	var skipEnd time.Time
	skipCount := 0

	printSkipped := func() {
		if skipCount > 0 && skipStart != nil {
			startStr := skipStart.Format("2006-01-02 15:04:05")
			duration := skipEnd.Sub(*skipStart) + hist.Granularity // Include the last bucket
			fmt.Printf("  %-20s | %12s | ... %d buckets skipped ...\n", startStr, "+"+formatDuration(duration), skipCount)
			skipCount = 0
			skipStart = nil
		}
	}

	for _, bucket := range hist.Buckets {
		// Always hide empty buckets, or those below threshold
		if bucket.Bytes == 0 || bucket.Throughput < threshold {
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

	// Print scale line at bottom
	printGraphScale(scalePrefix, graphWidth, maxTput, "B/s")

	fmt.Println()
}

// printRateStats prints the rate and throughput statistics
func printRateStats(stats RateStatistics, showRate, showThroughput bool) {
	fmt.Println("Statistics:")
	fmt.Printf("  Total Messages:                %s\n", humanize.Comma(int64(stats.TotalMessages)))
	fmt.Printf("  Total Messages (per seq nums):  %s\n", humanize.Comma(int64(stats.LastSeq-stats.FirstSeq+1)))
	fmt.Printf("  Total Data:                    %s\n", formatBytes(stats.TotalBytes))
	fmt.Printf("  Time Span:                     %s (%s to %s)\n",
		formatDuration(stats.TotalDuration),
		stats.StartTime.Format("2006-01-02 15:04:05"),
		stats.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("  Total Buckets:                 %s (active: %s, %.1f%%)\n",
		humanize.Comma(int64(stats.TotalBuckets)),
		humanize.Comma(int64(stats.ActiveBuckets)),
		float64(stats.ActiveBuckets)/float64(stats.TotalBuckets)*100)
	fmt.Println()

	if showRate {
		fmt.Println("  Message Storage Rate:")
		fmt.Printf("    Average:        %.2f msg/s\n", stats.AvgRate)
		fmt.Printf("    P50:            %.2f msg/s\n", stats.P50Rate)
		fmt.Printf("    P90:            %.2f msg/s\n", stats.P90Rate)
		fmt.Printf("    P99:            %.2f msg/s\n", stats.P99Rate)
		fmt.Printf("    P99.9:          %.2f msg/s\n", stats.P999Rate)
		fmt.Printf("    Min:            %.2f msg/s\n", stats.MinRate)
		fmt.Printf("    Max:            %.2f msg/s\n", stats.MaxRate)
		fmt.Printf("    Std Dev:        %.2f msg/s\n", stats.StdDevRate)
		fmt.Println()

		fmt.Println("  Message Storage Rate (per sequence numbers, with deletes interpolated):")
		fmt.Printf("    Average:        %.2f msg/s\n", stats.AvgSeqRate)
		fmt.Printf("    P50:            %.2f msg/s\n", stats.P50SeqRate)
		fmt.Printf("    P90:            %.2f msg/s\n", stats.P90SeqRate)
		fmt.Printf("    P99:            %.2f msg/s\n", stats.P99SeqRate)
		fmt.Printf("    P99.9:          %.2f msg/s\n", stats.P999SeqRate)
		fmt.Printf("    Min:            %.2f msg/s\n", stats.MinSeqRate)
		fmt.Printf("    Max:            %.2f msg/s\n", stats.MaxSeqRate)
		fmt.Printf("    Std Dev:        %.2f msg/s\n", stats.StdDevSeqRate)
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

	// Always show message size stats if we have messages
	if stats.TotalMessages > 0 {
		fmt.Println("  Message Size:")
		fmt.Printf("    Average:        %s\n", formatBytes(int64(stats.AvgMsgSize)))
		fmt.Printf("    P50:            %s\n", formatBytes(int64(stats.P50MsgSize)))
		fmt.Printf("    P90:            %s\n", formatBytes(int64(stats.P90MsgSize)))
		fmt.Printf("    P99:            %s\n", formatBytes(int64(stats.P99MsgSize)))
		fmt.Printf("    P99.9:          %s\n", formatBytes(int64(stats.P999MsgSize)))
		fmt.Printf("    Min:            %s\n", formatBytes(int64(stats.MinMsgSize)))
		fmt.Printf("    Max:            %s\n", formatBytes(int64(stats.MaxMsgSize)))
		fmt.Printf("    Std Dev:        %s\n", formatBytes(int64(stats.StdDevMsgSize)))
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

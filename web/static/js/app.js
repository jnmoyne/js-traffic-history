// JetStream Traffic History - Interactive GUI

(function() {
    'use strict';

    // State
    let currentStream = '';
    let rateChart = null;
    let throughputChart = null;
    let summaryData = null;
    let histogramData = null;
    let fullTimeRange = { min: null, max: null };  // Original time range for reset
    let currentZoom = { min: null, max: null };    // Current zoomed range
    let zoomHistory = [];                           // Stack of previous zoom levels
    let zoomRefetchTimeout = null;                  // Debounce timer for zoom refetch
    let isUpdatingData = false;                     // Flag to prevent refetch loop
    let requestVersion = 0;                         // Counter to ignore stale responses
    let currentAbortController = null;              // AbortController for cancelling in-flight requests
    let showInterpolatedDeletes = true;            // Toggle for interpolated deletes series
    let useAverageDownsampling = false;            // Toggle for average vs max downsampling

    // Formatters
    function formatNumber(n) {
        if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
        if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
        if (n >= 1e3) return (n / 1e3).toFixed(2) + 'K';
        return n.toFixed(2);
    }

    function formatBytes(bytes) {
        if (bytes === 0 || bytes == null || isNaN(bytes)) return '0 B';
        if (bytes < 0) bytes = 0;
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        // Bound index: max(0,...) for bytes < 1, min(...,len-1) for very large values
        const i = Math.max(0, Math.min(Math.floor(Math.log(bytes) / Math.log(k)), sizes.length - 1));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    }

    function formatDuration(ms) {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);

        if (days > 0) return `${days}d ${hours % 24}h ${minutes % 60}m`;
        if (hours > 0) return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
        if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
        return `${seconds}s`;
    }

    function formatTimestamp(ts) {
        const d = new Date(ts);
        return d.toLocaleString();
    }

    function formatTimestampShort(ts) {
        const d = new Date(ts);
        return d.toLocaleString(undefined, {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }

    function formatBucketDuration(ns) {
        const ms = ns / 1e6;
        const seconds = ms / 1000;
        const minutes = seconds / 60;
        const hours = minutes / 60;
        const days = hours / 24;

        if (days >= 1) return `${days.toFixed(1)}d`;
        if (hours >= 1) return `${hours.toFixed(1)}h`;
        if (minutes >= 1) return `${minutes.toFixed(1)}m`;
        if (seconds >= 1) return `${seconds.toFixed(1)}s`;
        return `${ms.toFixed(0)}ms`;
    }

    function updateBucketSizeIndicators(granularityNs, durationMs) {
        const durationText = formatDuration(durationMs);
        let text;
        if (useAverageDownsampling) {
            const bucketText = formatBucketDuration(granularityNs);
            text = `(${durationText}, downsampled to 5000 buckets of duration ${bucketText})`;
        } else {
            text = `(${durationText}, peaks preserved)`;
        }
        document.getElementById('rate-bucket-size').textContent = text;
        document.getElementById('throughput-bucket-size').textContent = text;
    }

    function updateZoomIndicators(isZoomed) {
        const rateIndicator = document.getElementById('rate-stats-zoom-indicator');
        const tputIndicator = document.getElementById('tput-stats-zoom-indicator');

        if (isZoomed && currentZoom.min !== null && currentZoom.max !== null) {
            const durationMs = (currentZoom.max - currentZoom.min) * 1000;
            const durationText = formatDuration(durationMs);
            const text = `(zoomed: ${durationText})`;
            rateIndicator.textContent = text;
            rateIndicator.classList.add('visible');
            tputIndicator.textContent = text;
            tputIndicator.classList.add('visible');
        } else {
            rateIndicator.classList.remove('visible');
            tputIndicator.classList.remove('visible');
        }
    }

    // API helpers
    async function fetchJSON(url, options = {}) {
        const response = await fetch(url, options);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();
    }

    // Loading overlay helpers
    function showLoadingOverlay(message) {
        const overlay = document.getElementById('loading-overlay');
        const text = overlay.querySelector('.loading-text');
        if (text) text.textContent = message || 'Loading data...';
        overlay.classList.remove('hidden');
    }

    function hideLoadingOverlay() {
        const overlay = document.getElementById('loading-overlay');
        overlay.classList.add('hidden');
    }

    function updateLoadingMessage(message) {
        const text = document.querySelector('#loading-overlay .loading-text');
        if (text) text.textContent = message;
    }

    // Zoom handling - refetch data at higher resolution for zoomed range
    function onZoomChange(min, max, fromHistory = false) {
        // Skip if we're currently updating data (prevents loop)
        if (isUpdatingData) {
            return;
        }

        // Check if this is a reset to full range
        const isFullRange = (min <= fullTimeRange.min && max >= fullTimeRange.max);

        let newZoom;
        if (isFullRange) {
            newZoom = { min: null, max: null };
        } else {
            newZoom = { min, max };
        }

        // Skip if zoom hasn't actually changed (with tolerance for float comparison)
        // Important: do this check BEFORE clearing the timeout to avoid canceling
        // a pending refetch when the synced chart fires with the same values
        const tolerance = 0.001;
        const minSame = (newZoom.min === null && currentZoom.min === null) ||
                        (newZoom.min !== null && currentZoom.min !== null &&
                         Math.abs(newZoom.min - currentZoom.min) < tolerance);
        const maxSame = (newZoom.max === null && currentZoom.max === null) ||
                        (newZoom.max !== null && currentZoom.max !== null &&
                         Math.abs(newZoom.max - currentZoom.max) < tolerance);

        if (minSame && maxSame) {
            return;
        }

        // Clear any pending refetch only when we're about to schedule a new one
        if (zoomRefetchTimeout) {
            clearTimeout(zoomRefetchTimeout);
        }

        // Push current zoom to history before changing (unless restoring from history)
        if (!fromHistory && currentZoom.min !== null && currentZoom.max !== null) {
            zoomHistory.push({ ...currentZoom });
        }

        currentZoom = newZoom;

        // Debounce the refetch to avoid too many requests while zooming
        zoomRefetchTimeout = setTimeout(async () => {
            try {
                await refetchHistogramForZoom();
            } catch (err) {
                console.error('refetchHistogramForZoom failed:', err);
            }
        }, 300);
    }

    // Go back to previous zoom level
    function zoomBack() {
        // Clear any pending refetch
        if (zoomRefetchTimeout) {
            clearTimeout(zoomRefetchTimeout);
            zoomRefetchTimeout = null;
        }

        // Capture version for this operation
        const thisVersion = ++requestVersion;

        if (zoomHistory.length > 0) {
            const previousZoom = zoomHistory.pop();
            // Set flag BEFORE setScale to prevent onZoomChange from pushing to history
            isUpdatingData = true;
            currentZoom = previousZoom;
            if (rateChart) {
                rateChart.setScale('x', {
                    min: previousZoom.min,
                    max: previousZoom.max
                });
            }
            setTimeout(() => {
                if (thisVersion !== requestVersion) return;
                isUpdatingData = false;
                // Trigger refetch for the restored zoom level
                zoomRefetchTimeout = setTimeout(async () => {
                    try {
                        await refetchHistogramForZoom();
                    } catch (err) {
                        if (err.name !== 'AbortError') {
                            console.error('refetchHistogramForZoom failed:', err);
                        }
                    }
                }, 100);
            }, 50);
        } else {
            // No history, go to full range
            if (rateChart && fullTimeRange.min !== null && fullTimeRange.max !== null) {
                isUpdatingData = true;
                currentZoom = { min: null, max: null };
                rateChart.setScale('x', {
                    min: fullTimeRange.min,
                    max: fullTimeRange.max
                });
                setTimeout(() => {
                    if (thisVersion !== requestVersion) return;
                    isUpdatingData = false;
                    // Trigger refetch for full range
                    zoomRefetchTimeout = setTimeout(async () => {
                        try {
                            await refetchHistogramForZoom();
                        } catch (err) {
                            if (err.name !== 'AbortError') {
                                console.error('refetchHistogramForZoom failed:', err);
                            }
                        }
                    }, 100);
                }, 50);
            }
        }
    }

    async function refetchHistogramForZoom() {
        // Increment version and capture it for this request
        const thisRequestVersion = ++requestVersion;

        // Abort any in-flight request
        if (currentAbortController) {
            currentAbortController.abort();
        }
        currentAbortController = new AbortController();

        try {
            let url = currentStream
                ? `/api/histogram?stream=${encodeURIComponent(currentStream)}`
                : '/api/histogram';

            if (currentZoom.min !== null && currentZoom.max !== null) {
                url += (url.includes('?') ? '&' : '?') + `start=${currentZoom.min}&end=${currentZoom.max}`;
            }

            if (useAverageDownsampling) {
                url += (url.includes('?') ? '&' : '?') + 'downsample=avg';
            }

            const data = await fetchJSON(url, { signal: currentAbortController.signal });

            // Check if this response is still relevant (no newer request has started)
            if (thisRequestVersion !== requestVersion) {
                return;
            }

            if (!data || !data.buckets || data.buckets.length === 0) {
                // Still update distribution to show "no data" message
                if (!currentStream) {
                    updateDistributionFromBuckets([]);
                }
                return;
            }

            // Update histogramData so tooltip can access the correct bucket data
            histogramData = data;

            // Set flag to prevent refetch loop when updating data
            isUpdatingData = true;

            try {
                // Update rate chart data
                if (rateChart) {
                    const timestamps = data.buckets.map(b => new Date(b.start).getTime() / 1000);
                    const storedRates = data.buckets.map(b => b.rate);
                    const totalRates = data.buckets.map(b => b.seq_rate);
                    const deletedRates = data.buckets.map(b => b.seq_rate - b.rate);
                    rateChart.setData([timestamps, storedRates, totalRates, deletedRates]);
                }

                // Update throughput chart data
                if (throughputChart) {
                    const timestamps = data.buckets.map(b => new Date(b.start).getTime() / 1000);
                    const throughputs = data.buckets.map(b => b.throughput);
                    throughputChart.setData([timestamps, throughputs]);
                }

                // Update statistics from the API response
                // The server calculates stats for the filtered range
                if (data.stats) {
                    updateRateStats(data.stats);
                    updateThroughputStats(data.stats);
                }

                // Update bucket size indicator
                if (data.granularity_ns && data.stats) {
                    const durationMs = data.stats.total_duration_ns / 1e6;
                    updateBucketSizeIndicators(data.granularity_ns, durationMs);
                }

                // Update zoom indicators
                const isZoomed = currentZoom.min !== null && currentZoom.max !== null;
                updateZoomIndicators(isZoomed);

                // Update distribution from bucket data if viewing combined
                if (!currentStream && data.buckets) {
                    updateDistributionFromBuckets(data.buckets);
                }
            } finally {
                // Clear flag after a short delay to allow any triggered events to be ignored
                setTimeout(() => {
                    // Only clear if we're still the current request
                    if (thisRequestVersion === requestVersion) {
                        isUpdatingData = false;
                    }
                }, 100);
            }
        } catch (err) {
            // Ignore abort errors
            if (err.name === 'AbortError') {
                return;
            }
            console.error('Failed to refetch histogram for zoom:', err);
            if (thisRequestVersion === requestVersion) {
                isUpdatingData = false;
            }
        }
    }

    // Toggle interpolated deletes series visibility
    function toggleInterpolatedDeletes(show) {
        showInterpolatedDeletes = show;
        if (rateChart) {
            // Series 2 is "Stored + Deleted", Series 3 is "Deleted Rate"
            rateChart.setSeries(2, { show: show });
            rateChart.setSeries(3, { show: show });
        }
    }

    // Chart helpers
    function getChartColors() {
        return {
            stored: '#4ecdc4',
            deleted: '#ff6b6b',
            total: '#ffd93d',
            throughput: '#a78bfa',
            grid: 'rgba(255,255,255,0.1)',
            axis: 'rgba(255,255,255,0.5)',
            text: '#a0a0a0'
        };
    }

    // Shared tooltip elements
    let rateTooltip = null;
    let throughputTooltip = null;

    function updateRateTooltip(idx, cursorLeft, cursorTop, chartRect) {
        if (!rateTooltip || !rateChart) return;

        if (idx == null) {
            rateTooltip.style.display = 'none';
            return;
        }

        const colors = getChartColors();
        const ts = rateChart.data[0][idx];
        const stored = rateChart.data[1][idx];
        const total = rateChart.data[2][idx];
        const deleted = rateChart.data[3][idx];

        let streamsHtml = '';
        // Show per-stream activity when viewing combined (all streams)
        if (!currentStream && histogramData && histogramData.buckets && histogramData.buckets[idx]) {
            const bucket = histogramData.buckets[idx];
            const perStream = bucket.per_stream;
            if (perStream) {
                const granularitySecs = histogramData.granularity_ns / 1e9;
                const streamEntries = Object.entries(perStream)
                    .filter(([_, data]) => data.seq_count > 0)
                    .sort((a, b) => b[1].seq_count - a[1].seq_count);
                if (streamEntries.length > 0) {
                    streamsHtml = '<div class="tooltip-streams"><div class="tooltip-streams-header">Streams (stored msgs/s):</div>';
                    const maxToShow = 8;
                    for (let i = 0; i < Math.min(streamEntries.length, maxToShow); i++) {
                        const [name, data] = streamEntries[i];
                        const storedRate = data.count / granularitySecs;
                        streamsHtml += `<div class="tooltip-stream-row"><span class="tooltip-stream-name">${name}:</span> <span class="tooltip-stream-values"><span style="color:${colors.stored}">${formatNumber(storedRate)}</span></span></div>`;
                    }
                    if (streamEntries.length > maxToShow) {
                        streamsHtml += `<div class="tooltip-stream-more">+${streamEntries.length - maxToShow} more</div>`;
                    }
                    streamsHtml += '</div>';
                }
            }
        }

        rateTooltip.innerHTML = `
            <div class="tooltip-time">${formatTimestampShort(ts * 1000)}</div>
            <div class="tooltip-row"><span style="color:${colors.stored}">Stored:</span> ${formatNumber(stored)} msg/s</div>
            <div class="tooltip-row"><span style="color:${colors.total}">Stored + Deleted:</span> ${formatNumber(total)} msg/s</div>
            <div class="tooltip-row"><span style="color:${colors.deleted}">Deleted:</span> ${formatNumber(deleted)} msg/s</div>
            ${streamsHtml}
        `;

        const left = chartRect.left + cursorLeft;
        const top = chartRect.top + cursorTop;

        rateTooltip.style.display = 'block';
        rateTooltip.style.left = (left + 15) + 'px';
        rateTooltip.style.top = (top - 10) + 'px';

        const tooltipRect = rateTooltip.getBoundingClientRect();
        if (tooltipRect.right > window.innerWidth) {
            rateTooltip.style.left = (left - tooltipRect.width - 15) + 'px';
        }
    }

    function updateThroughputTooltip(idx, cursorLeft, cursorTop, chartRect) {
        if (!throughputTooltip || !throughputChart) return;

        if (idx == null) {
            throughputTooltip.style.display = 'none';
            return;
        }

        const colors = getChartColors();
        const ts = throughputChart.data[0][idx];
        const tput = throughputChart.data[1][idx];

        let streamsHtml = '';
        // Show per-stream activity when viewing combined (all streams)
        if (!currentStream && histogramData && histogramData.buckets && histogramData.buckets[idx]) {
            const bucket = histogramData.buckets[idx];
            const perStream = bucket.per_stream;
            if (perStream) {
                const streamEntries = Object.entries(perStream)
                    .filter(([_, data]) => data.bytes > 0)
                    .sort((a, b) => b[1].bytes - a[1].bytes);
                if (streamEntries.length > 0) {
                    streamsHtml = '<div class="tooltip-streams"><div class="tooltip-streams-header">Streams:</div>';
                    const maxToShow = 5;
                    for (let i = 0; i < Math.min(streamEntries.length, maxToShow); i++) {
                        const [name, data] = streamEntries[i];
                        streamsHtml += `<div class="tooltip-stream-row">${name}: ${formatBytes(data.bytes)}</div>`;
                    }
                    if (streamEntries.length > maxToShow) {
                        streamsHtml += `<div class="tooltip-stream-more">+${streamEntries.length - maxToShow} more</div>`;
                    }
                    streamsHtml += '</div>';
                }
            }
        }

        throughputTooltip.innerHTML = `
            <div class="tooltip-time">${formatTimestampShort(ts * 1000)}</div>
            <div class="tooltip-row"><span style="color:${colors.throughput}">Throughput:</span> ${formatBytes(tput)}/s</div>
            ${streamsHtml}
        `;

        const left = chartRect.left + cursorLeft;
        const top = chartRect.top + cursorTop;

        throughputTooltip.style.display = 'block';
        throughputTooltip.style.left = (left + 15) + 'px';
        throughputTooltip.style.top = (top - 10) + 'px';

        const tooltipRect = throughputTooltip.getBoundingClientRect();
        if (tooltipRect.right > window.innerWidth) {
            throughputTooltip.style.left = (left - tooltipRect.width - 15) + 'px';
        }
    }

    function updateBothTooltips(idx) {
        if (idx == null) {
            if (rateTooltip) rateTooltip.style.display = 'none';
            if (throughputTooltip) throughputTooltip.style.display = 'none';
            return;
        }

        // Update rate tooltip using rate chart's cursor position
        if (rateChart && rateTooltip) {
            const rateRect = rateChart.over.getBoundingClientRect();
            const rateCursorLeft = rateChart.valToPos(rateChart.data[0][idx], 'x');
            const rateCursorTop = rateChart.valToPos(rateChart.data[1][idx], 'y');
            updateRateTooltip(idx, rateCursorLeft, rateCursorTop, rateRect);
        }

        // Update throughput tooltip using throughput chart's cursor position
        if (throughputChart && throughputTooltip) {
            const tputRect = throughputChart.over.getBoundingClientRect();
            const tputCursorLeft = throughputChart.valToPos(throughputChart.data[0][idx], 'x');
            const tputCursorTop = throughputChart.valToPos(throughputChart.data[1][idx], 'y');
            updateThroughputTooltip(idx, tputCursorLeft, tputCursorTop, tputRect);
        }
    }

    function createRateChart(container, data) {
        if (!data || !data.buckets || data.buckets.length === 0) {
            container.innerHTML = '<div class="error">No rate data available</div>';
            return null;
        }

        const colors = getChartColors();
        const timestamps = data.buckets.map(b => new Date(b.start).getTime() / 1000);
        const storedRates = data.buckets.map(b => b.rate);
        const totalRates = data.buckets.map(b => b.seq_rate);
        const deletedRates = data.buckets.map(b => b.seq_rate - b.rate);

        // Create shared tooltip element if not exists
        if (!rateTooltip) {
            rateTooltip = document.createElement('div');
            rateTooltip.className = 'chart-tooltip';
            document.body.appendChild(rateTooltip);
        }

        const opts = {
            width: container.clientWidth,
            height: 280,
            title: '',
            cursor: {
                sync: {
                    key: 'traffic-sync',
                    setSeries: true
                },
                drag: {
                    x: true,
                    y: false,
                    setScale: true
                },
                focus: {
                    prox: -1
                },
                points: {
                    size: 8,
                    fill: (u, seriesIdx) => u.series[seriesIdx].stroke(),
                    stroke: (u, seriesIdx) => '#fff',
                    width: 2
                }
            },
            select: {
                show: true,
                left: 0,
                width: 0,
            },
            scales: {
                x: { time: true },
                y: { auto: true, range: [0, null] }
            },
            axes: [
                {
                    stroke: colors.axis,
                    grid: { stroke: colors.grid },
                    ticks: { stroke: colors.grid }
                },
                {
                    stroke: colors.axis,
                    grid: { stroke: colors.grid },
                    ticks: { stroke: colors.grid },
                    size: 60,
                    values: (u, vals) => vals.map(v => formatNumber(v))
                }
            ],
            series: [
                {
                    value: (u, v) => v == null ? '-' : formatTimestampShort(v * 1000)
                },
                {
                    label: 'Stored Rate',
                    stroke: colors.stored,
                    fill: colors.stored + '40',
                    width: 2,
                    points: { show: false },
                    value: (u, v) => v == null ? '-' : formatNumber(v) + ' msg/s'
                },
                {
                    label: 'Stored + Deleted',
                    stroke: colors.total,
                    fill: colors.total + '20',
                    width: 2,
                    points: { show: false },
                    show: showInterpolatedDeletes,
                    value: (u, v) => v == null ? '-' : formatNumber(v) + ' msg/s'
                },
                {
                    label: 'Deleted Rate',
                    stroke: colors.deleted,
                    fill: colors.deleted + '30',
                    width: 1,
                    dash: [4, 4],
                    points: { show: false },
                    show: showInterpolatedDeletes,
                    value: (u, v) => v == null ? '-' : formatNumber(v) + ' msg/s'
                }
            ],
            legend: {
                show: true,
                live: true
            },
            hooks: {
                setSelect: [
                    function(u) {
                        if (u.select.width > 0) {
                            let min = u.posToVal(u.select.left, 'x');
                            let max = u.posToVal(u.select.left + u.select.width, 'x');
                            u.setScale('x', { min, max });
                        }
                        u.setSelect({ left: 0, width: 0 }, false);
                    }
                ],
                setScale: [
                    function(u, key) {
                        if (key === 'x') {
                            const min = u.scales.x.min;
                            const max = u.scales.x.max;
                            onZoomChange(min, max);
                        }
                    }
                ],
                setCursor: [
                    function(u) {
                        const { idx } = u.cursor;
                        updateBothTooltips(idx);
                    }
                ],
            }
        };

        container.innerHTML = '';
        const chart = new uPlot(opts, [timestamps, storedRates, totalRates, deletedRates], container);

        // Double-click to go back to previous zoom level
        container.addEventListener('dblclick', () => {
            zoomBack();
        });

        // Handle resize
        const resizeObserver = new ResizeObserver(entries => {
            for (let entry of entries) {
                chart.setSize({
                    width: entry.contentRect.width,
                    height: 280
                });
            }
        });
        resizeObserver.observe(container);

        return chart;
    }

    function createThroughputChart(container, data) {
        if (!data || !data.buckets || data.buckets.length === 0) {
            container.innerHTML = '<div class="error">No throughput data available</div>';
            return null;
        }

        const colors = getChartColors();
        const timestamps = data.buckets.map(b => new Date(b.start).getTime() / 1000);
        const throughputs = data.buckets.map(b => b.throughput);

        // Create shared tooltip element if not exists
        if (!throughputTooltip) {
            throughputTooltip = document.createElement('div');
            throughputTooltip.className = 'chart-tooltip';
            document.body.appendChild(throughputTooltip);
        }

        const opts = {
            width: container.clientWidth,
            height: 280,
            title: '',
            cursor: {
                sync: {
                    key: 'traffic-sync',
                    setSeries: true
                },
                drag: {
                    x: true,
                    y: false,
                    setScale: true
                },
                focus: {
                    prox: -1
                },
                points: {
                    size: 8,
                    fill: (u, seriesIdx) => u.series[seriesIdx].stroke(),
                    stroke: (u, seriesIdx) => '#fff',
                    width: 2
                }
            },
            select: {
                show: true,
                left: 0,
                width: 0,
            },
            scales: {
                x: { time: true },
                y: { auto: true, range: [0, null] }
            },
            axes: [
                {
                    stroke: colors.axis,
                    grid: { stroke: colors.grid },
                    ticks: { stroke: colors.grid }
                },
                {
                    stroke: colors.axis,
                    grid: { stroke: colors.grid },
                    ticks: { stroke: colors.grid },
                    size: 80,
                    values: (u, vals) => vals.map(v => formatBytes(v) + '/s')
                }
            ],
            series: [
                {
                    value: (u, v) => v == null ? '-' : formatTimestampShort(v * 1000)
                },
                {
                    label: 'Throughput',
                    stroke: colors.throughput,
                    fill: colors.throughput + '40',
                    width: 2,
                    points: { show: false },
                    value: (u, v) => v == null ? '-' : formatBytes(v) + '/s'
                }
            ],
            legend: {
                show: true,
                live: true
            },
            hooks: {
                setSelect: [
                    function(u) {
                        if (u.select.width > 0) {
                            let min = u.posToVal(u.select.left, 'x');
                            let max = u.posToVal(u.select.left + u.select.width, 'x');
                            u.setScale('x', { min, max });
                        }
                        u.setSelect({ left: 0, width: 0 }, false);
                    }
                ],
                setScale: [
                    function(u, key) {
                        if (key === 'x') {
                            const min = u.scales.x.min;
                            const max = u.scales.x.max;
                            onZoomChange(min, max);
                        }
                    }
                ],
                setCursor: [
                    function(u) {
                        const { idx } = u.cursor;
                        updateBothTooltips(idx);
                    }
                ],
            }
        };

        container.innerHTML = '';
        const chart = new uPlot(opts, [timestamps, throughputs], container);

        // Double-click to go back to previous zoom level
        container.addEventListener('dblclick', () => {
            zoomBack();
        });

        // Handle resize
        const resizeObserver = new ResizeObserver(entries => {
            for (let entry of entries) {
                chart.setSize({
                    width: entry.contentRect.width,
                    height: 280
                });
            }
        });
        resizeObserver.observe(container);

        return chart;
    }

    // Calculate stream distribution from histogram bucket data
    function calculateDistributionFromBuckets(buckets) {
        const streamTotals = {};

        for (const bucket of buckets) {
            if (bucket.per_stream) {
                for (const [streamName, data] of Object.entries(bucket.per_stream)) {
                    if (!streamTotals[streamName]) {
                        streamTotals[streamName] = { name: streamName, messages: 0, bytes: 0 };
                    }
                    streamTotals[streamName].messages += data.count;
                    streamTotals[streamName].bytes += data.bytes;
                }
            }
        }

        // Convert to array and sort by message count descending
        const streams = Object.values(streamTotals);
        streams.sort((a, b) => b.messages - a.messages);
        return streams;
    }

    function updateDistributionFromBuckets(buckets) {
        const distSection = document.getElementById('distribution-section');
        if (!distSection) {
            return;
        }
        if (distSection.style.display === 'none') {
            return;
        }

        const streams = calculateDistributionFromBuckets(buckets);
        const distContainer = document.getElementById('distribution-list');

        if (distContainer) {
            if (streams.length > 0) {
                createDistributionList(distContainer, streams);
            } else {
                distContainer.innerHTML = '<div class="no-data">No streams with data in this time range</div>';
            }
        }
    }

    function createDistributionList(container, streams) {
        if (!streams || streams.length === 0) {
            container.innerHTML = '<div class="error">No stream distribution data available</div>';
            return;
        }

        // Sort streams by message count descending
        const sortedStreams = [...streams].sort((a, b) => b.messages - a.messages);
        const maxMsg = Math.max(...sortedStreams.map(s => s.messages));

        // Build HTML list
        let html = '';
        for (const stream of sortedStreams) {
            const barWidth = maxMsg > 0 ? (stream.messages / maxMsg * 100) : 0;
            html += `
                <div class="distribution-item">
                    <div class="stream-name" title="${stream.name}">${stream.name}</div>
                    <div class="bar-container">
                        <div class="bar" style="width: ${barWidth}%"></div>
                    </div>
                    <div class="stream-stats">
                        <span class="msg-count">${formatNumber(stream.messages)}</span> msgs / ${formatBytes(stream.bytes)}
                    </div>
                </div>
            `;
        }

        container.innerHTML = html;
    }

    // UI update functions
    function updateSummary(summary, streamName) {
        if (!summary) return;

        // If a specific stream is selected, find its data
        if (streamName && summary.streams) {
            const streamData = summary.streams.find(s => s.name === streamName);
            if (streamData) {
                // Show stream-specific data
                document.getElementById('summary-duration').textContent =
                    formatDuration(summary.duration_ns / 1e6);
                document.getElementById('summary-timerange').textContent =
                    `${formatTimestamp(summary.start_time)} - ${formatTimestamp(summary.end_time)}`;
                document.getElementById('summary-streams').textContent = '1 (of ' + summary.stream_count + ')';
                document.getElementById('summary-messages').textContent = formatNumber(streamData.messages);
                document.getElementById('summary-bytes').textContent = formatBytes(streamData.bytes);

                if (summary.duration_ns > 0) {
                    const avgThroughput = streamData.bytes / (summary.duration_ns / 1e9);
                    document.getElementById('summary-throughput').textContent = formatBytes(avgThroughput) + '/s';
                } else {
                    document.getElementById('summary-throughput').textContent = '-';
                }
                return;
            }
        }

        // Show combined data
        document.getElementById('summary-duration').textContent =
            formatDuration(summary.duration_ns / 1e6);
        document.getElementById('summary-timerange').textContent =
            `${formatTimestamp(summary.start_time)} - ${formatTimestamp(summary.end_time)}`;
        document.getElementById('summary-streams').textContent = summary.stream_count;
        document.getElementById('summary-messages').textContent = formatNumber(summary.total_msgs);
        document.getElementById('summary-bytes').textContent = formatBytes(summary.total_bytes);

        if (summary.duration_ns > 0) {
            const avgThroughput = summary.total_bytes / (summary.duration_ns / 1e9);
            document.getElementById('summary-throughput').textContent = formatBytes(avgThroughput) + '/s';
        } else {
            document.getElementById('summary-throughput').textContent = '-';
        }
    }

    function updateSummaryFromStats(stats) {
        if (!stats) return;
        // Update throughput from histogram stats (more accurate than total_bytes/duration)
        document.getElementById('summary-throughput').textContent = formatBytes(stats.avg_throughput) + '/s';
    }

    function updateRateStats(stats) {
        if (!stats) return;

        document.getElementById('rate-avg').textContent = formatNumber(stats.avg_rate);
        document.getElementById('rate-p50').textContent = formatNumber(stats.p50_rate);
        document.getElementById('rate-p90').textContent = formatNumber(stats.p90_rate);
        document.getElementById('rate-p99').textContent = formatNumber(stats.p99_rate);
        document.getElementById('rate-p999').textContent = formatNumber(stats.p999_rate);
        document.getElementById('rate-min').textContent = formatNumber(stats.min_rate);
        document.getElementById('rate-max').textContent = formatNumber(stats.max_rate);
        document.getElementById('rate-stddev').textContent = formatNumber(stats.stddev_rate);

        document.getElementById('seqrate-avg').textContent = formatNumber(stats.avg_seq_rate);
        document.getElementById('seqrate-p50').textContent = formatNumber(stats.p50_seq_rate);
        document.getElementById('seqrate-p90').textContent = formatNumber(stats.p90_seq_rate);
        document.getElementById('seqrate-p99').textContent = formatNumber(stats.p99_seq_rate);
        document.getElementById('seqrate-p999').textContent = formatNumber(stats.p999_seq_rate);
        document.getElementById('seqrate-min').textContent = formatNumber(stats.min_seq_rate);
        document.getElementById('seqrate-max').textContent = formatNumber(stats.max_seq_rate);
        document.getElementById('seqrate-stddev').textContent = formatNumber(stats.stddev_seq_rate);
    }

    function updateThroughputStats(stats) {
        if (!stats) return;

        document.getElementById('tput-avg').textContent = formatBytes(stats.avg_throughput) + '/s';
        document.getElementById('tput-p50').textContent = formatBytes(stats.p50_throughput) + '/s';
        document.getElementById('tput-p90').textContent = formatBytes(stats.p90_throughput) + '/s';
        document.getElementById('tput-p99').textContent = formatBytes(stats.p99_throughput) + '/s';
        document.getElementById('tput-p999').textContent = formatBytes(stats.p999_throughput) + '/s';
        document.getElementById('tput-min').textContent = formatBytes(stats.min_throughput) + '/s';
        document.getElementById('tput-max').textContent = formatBytes(stats.max_throughput) + '/s';
        document.getElementById('tput-stddev').textContent = formatBytes(stats.stddev_throughput) + '/s';

        document.getElementById('msgsize-avg').textContent = formatBytes(stats.avg_msg_size);
        document.getElementById('msgsize-p50').textContent = formatBytes(stats.p50_msg_size);
        document.getElementById('msgsize-p90').textContent = formatBytes(stats.p90_msg_size);
        document.getElementById('msgsize-p99').textContent = formatBytes(stats.p99_msg_size);
        document.getElementById('msgsize-p999').textContent = formatBytes(stats.p999_msg_size);
        document.getElementById('msgsize-min').textContent = formatBytes(stats.min_msg_size);
        document.getElementById('msgsize-max').textContent = formatBytes(stats.max_msg_size);
        document.getElementById('msgsize-stddev').textContent = formatBytes(stats.stddev_msg_size);
    }

    function showLoading(containerId) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = '<div class="loading">Loading...</div>';
        }
    }

    function showError(containerId, message) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = `<div class="error">${message}</div>`;
        }
    }

    // Main data loading functions
    async function loadStreams() {
        try {
            const streams = await fetchJSON('/api/streams');
            const select = document.getElementById('stream-select');

            // Clear existing options except the first one
            while (select.options.length > 1) {
                select.remove(1);
            }

            // Add stream options (sorted alphabetically)
            if (streams && streams.length > 0) {
                const sortedStreams = [...streams].sort((a, b) => a.localeCompare(b));
                sortedStreams.forEach(stream => {
                    const option = document.createElement('option');
                    option.value = stream;
                    option.textContent = stream;
                    select.appendChild(option);
                });
            }
        } catch (err) {
            console.error('Failed to load streams:', err);
        }
    }

    async function loadSummary() {
        try {
            summaryData = await fetchJSON('/api/summary');
            updateSummary(summaryData, currentStream);

            // Create distribution list if we have stream data
            if (summaryData && summaryData.streams && summaryData.streams.length > 1) {
                const distContainer = document.getElementById('distribution-list');
                createDistributionList(distContainer, summaryData.streams);
                document.getElementById('distribution-section').style.display = 'block';
            } else {
                document.getElementById('distribution-section').style.display = 'none';
            }
        } catch (err) {
            console.error('Failed to load summary:', err);
        }
    }

    async function loadHistogram(stream) {
        showLoading('rate-chart');
        showLoading('throughput-chart');

        // Increment version to invalidate any pending operations
        const thisRequestVersion = ++requestVersion;

        // Save current zoom state to restore after loading new stream
        const savedZoom = (currentZoom.min !== null && currentZoom.max !== null)
            ? { min: currentZoom.min, max: currentZoom.max }
            : null;

        // Clear pending refetch timeout
        if (zoomRefetchTimeout) {
            clearTimeout(zoomRefetchTimeout);
            zoomRefetchTimeout = null;
        }

        // Abort any in-flight request
        if (currentAbortController) {
            currentAbortController.abort();
        }
        currentAbortController = new AbortController();

        // Reset the updating flag since we're starting fresh
        isUpdatingData = false;

        try {
            // Always fetch full data first to get correct fullTimeRange
            let url = stream ? `/api/histogram?stream=${encodeURIComponent(stream)}` : '/api/histogram';
            if (useAverageDownsampling) {
                url += (url.includes('?') ? '&' : '?') + 'downsample=avg';
            }
            histogramData = await fetchJSON(url, { signal: currentAbortController.signal });

            // Check if this response is still relevant
            if (thisRequestVersion !== requestVersion) {
                return;
            }

            // Store full time range for zoom reset detection
            if (histogramData && histogramData.buckets && histogramData.buckets.length > 0) {
                const timestamps = histogramData.buckets.map(b => new Date(b.start).getTime() / 1000);
                fullTimeRange = {
                    min: Math.min(...timestamps),
                    max: Math.max(...timestamps)
                };
            }

            // Create rate chart
            const rateContainer = document.getElementById('rate-chart');
            if (rateChart) {
                rateChart.destroy();
            }
            rateChart = createRateChart(rateContainer, histogramData);

            // Create throughput chart
            const tputContainer = document.getElementById('throughput-chart');
            if (throughputChart) {
                throughputChart.destroy();
            }
            throughputChart = createThroughputChart(tputContainer, histogramData);

            // Update stats
            if (histogramData && histogramData.stats) {
                updateRateStats(histogramData.stats);
                updateThroughputStats(histogramData.stats);
                updateSummaryFromStats(histogramData.stats);
            }

            // Update bucket size indicator
            if (histogramData && histogramData.granularity_ns && histogramData.stats) {
                const durationMs = histogramData.stats.total_duration_ns / 1e6;
                updateBucketSizeIndicators(histogramData.granularity_ns, durationMs);
            }

            // Update distribution from bucket data if viewing combined
            if (!stream && histogramData && histogramData.buckets) {
                updateDistributionFromBuckets(histogramData.buckets);
            }

            // Restore zoom if we had one and it's within the new data's time range
            if (savedZoom && fullTimeRange.min !== null && fullTimeRange.max !== null) {
                // Check if saved zoom overlaps with new data's time range
                if (savedZoom.max > fullTimeRange.min && savedZoom.min < fullTimeRange.max) {
                    // Clamp zoom to the new data's range
                    const clampedZoom = {
                        min: Math.max(savedZoom.min, fullTimeRange.min),
                        max: Math.min(savedZoom.max, fullTimeRange.max)
                    };
                    currentZoom = clampedZoom;
                    isUpdatingData = true;
                    if (rateChart) {
                        rateChart.setScale('x', {
                            min: clampedZoom.min,
                            max: clampedZoom.max
                        });
                    }
                    setTimeout(() => {
                        // Only proceed if we're still the current request
                        if (thisRequestVersion !== requestVersion) {
                            return;
                        }
                        isUpdatingData = false;
                        // Trigger refetch for the restored zoom level
                        zoomRefetchTimeout = setTimeout(async () => {
                            try {
                                await refetchHistogramForZoom();
                            } catch (err) {
                                if (err.name !== 'AbortError') {
                                    console.error('refetchHistogramForZoom failed:', err);
                                }
                            }
                        }, 100);
                    }, 50);
                } else {
                    // Saved zoom doesn't overlap with new data, reset zoom
                    currentZoom = { min: null, max: null };
                    updateZoomIndicators(false);
                }
            } else {
                // No saved zoom, show full range
                currentZoom = { min: null, max: null };
                updateZoomIndicators(false);
            }
        } catch (err) {
            // Ignore abort errors
            if (err.name === 'AbortError') {
                return;
            }
            console.error('Failed to load histogram:', err);
            showError('rate-chart', 'Failed to load rate data');
            showError('throughput-chart', 'Failed to load throughput data');
        }
    }

    // Event handlers
    async function onStreamChange(event) {
        currentStream = event.target.value;
        showLoadingOverlay('Loading ' + (currentStream || 'combined') + ' data...');
        try {
            // Update summary for selected stream
            updateSummary(summaryData, currentStream);

            // Show/hide distribution section (only for combined view)
            const distSection = document.getElementById('distribution-section');
            if (currentStream) {
                distSection.style.display = 'none';
            } else if (summaryData && summaryData.streams && summaryData.streams.length > 1) {
                distSection.style.display = 'block';
            }

            await loadHistogram(currentStream);
            hideLoadingOverlay();
        } catch (err) {
            // Ignore abort errors (happens when rapidly switching streams)
            if (err.name === 'AbortError') {
                return;
            }
            console.error('Failed to load histogram:', err);
            updateLoadingMessage('Error: ' + err.message);
            setTimeout(() => hideLoadingOverlay(), 5000);
        }
    }

    // Collapsible section functionality
    function initCollapsibleSections() {
        const headers = document.querySelectorAll('.collapsible .section-header');
        headers.forEach(header => {
            header.addEventListener('click', function(e) {
                // Don't toggle if clicking on the zoom indicator
                if (e.target.classList.contains('zoom-indicator')) return;

                const section = this.closest('.collapsible');
                section.classList.toggle('collapsed');
            });
        });
    }

    // Initialization
    async function init() {
        // Set up collapsible sections
        initCollapsibleSections();

        // Set up event listeners
        document.getElementById('stream-select').addEventListener('change', onStreamChange);

        // Set up interpolated deletes checkbox
        const interpolatedCheckbox = document.getElementById('show-interpolated-deletes');
        if (interpolatedCheckbox) {
            interpolatedCheckbox.addEventListener('change', (e) => {
                toggleInterpolatedDeletes(e.target.checked);
            });
        }

        // Set up downsampling checkbox
        const avgDownsamplingCheckbox = document.getElementById('average-downsampling');
        if (avgDownsamplingCheckbox) {
            avgDownsamplingCheckbox.addEventListener('change', async (e) => {
                useAverageDownsampling = e.target.checked;
                // Refetch data with new downsampling mode
                await refetchHistogramForZoom();
            });
        }

        // Hide tooltips when mouse leaves all chart areas
        document.addEventListener('mousemove', (e) => {
            const overAnyChart = e.target.closest('.chart-container');
            if (!overAnyChart) {
                if (rateTooltip) rateTooltip.style.display = 'none';
                if (throughputTooltip) throughputTooltip.style.display = 'none';
            }
        });

        try {
            showLoadingOverlay('Loading stream list...');

            // Load initial data
            await loadStreams();

            updateLoadingMessage('Loading summary...');
            await loadSummary();

            updateLoadingMessage('Loading histogram data...');
            // Load combined histogram
            await loadHistogram('');

            hideLoadingOverlay();
        } catch (err) {
            console.error('Failed to initialize:', err);
            updateLoadingMessage('Error: ' + err.message);
            // Don't hide overlay on error - let user see the message
            setTimeout(() => hideLoadingOverlay(), 5000);
        }
    }

    // Start the app when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();

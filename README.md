# js-traffic-history
Tool to create statistics and a history of NATS JetStream message traffic from the data in the limits streams (or just for a specific stream).

Sample basic output:

```
Connecting to NATS...
Discovering streams with limits retention policy...
Found 1 stream(s) to analyze

Fetching messages from stream: benchstream (207108 messages)
                                                            
======================================================================
TRAFFIC HISTORY REPORT
======================================================================

Overview:
  Start Time:       2026-01-17 13:29:21.538
  End Time:         2026-01-18 00:19:07.863
  Duration:         10h49m46.3s
  Streams:          1
  Total Messages:   207108
  Total Data:       25.3 MB
  Avg Throughput:   679 B/s
  Avg Rate:         5.31 msg/s

Streams by Message Count:
  benchstream            207108 msgs    25.3 MB | ████████████████████████████████████████

----------------------------------------------------------------------
COMBINED (1 streams, 207108 total messages)
----------------------------------------------------------------------

-- Message Rate Over Time (granularity: 1.00s) ----------------------

Statistics:
  Total Messages:   207108
  Total Data:       25.3 MB
  Time Span:        10h49m47.0s
  Total Buckets:    38987 (active: 184, 0.5%)

  Message Rate:
    Average:        5.31 msg/s
    P50:            0.00 msg/s
    P90:            0.00 msg/s
    P99:            0.00 msg/s
    P99.9:          47.00 msg/s
    Min:            0.00 msg/s
    Max:            13530.00 msg/s
    Std Dev:        234.96 msg/s

  Throughput:
    Average:        679 B/s
    P50:            0 B/s
    P90:            0 B/s
    P99:            0 B/s
    P99.9:          5.9 KB/s
    Min:            0 B/s
    Max:            1.7 MB/s
    Std Dev:        29.4 KB/s
```

Can also create ascii time series and generate CSV files.

Note: Running this can be impactful on the NATS servers as it will try to get every single message from every single limits stream within the specified time interval (one by one and using batched direct gets to try and limit the impact).
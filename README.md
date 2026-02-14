# js-traffic-history
Tool to create statistics and a history of NATS JetStream message traffic from the data in the limits streams (or just for a specific stream) for an account.

## How it works

The tool can only work on the data that is still stored in the streams at the time you run it, meaning it doesn't have detailed info about deleted messages. It does however try to remediate some of that by interpolating 'interior deletes' from the gaps in sequence numbers.

The tool only reports on stream publication rate which is only part of the load incurred on the servers, as there are typically also consumers being used (and therefore incurring load on the servers) at the same time.

## Usage

```
usage: js-traffic-history [<flags>]

Analyze stored message rates across NATS JetStream for accessible streams in the account (with limits retention policy)

Global Flags:
      --help               Show context-sensitive help
      --version            Show application version.
  -c, --context=CONTEXT    NATS context name (uses default if empty)
      --granularity=1s     Time bucket size for rate calculation
  -g, --[no-]graph         Display ASCII graph
      --[no-]rate          Show message rate graph and stats
      --[no-]throughput    Show throughput graph and stats
  -s, --stream=STREAM ...  Analyze specific stream(s) (can be repeated)
      --batch-size=10000   Messages per batch request
  -l, --limit=0            Max messages to analyze per stream (0 = all)
      --[no-]per-stream    Also show stats and graphs for each individual stream
      --csv=CSV            Export histogram data to CSV file
      --min-rate-pct=10    Skip graph buckets below this percentage of max rate
      --start=START        Start timestamp (RFC3339 or 2006-01-02 15:04:05)
      --end=END            End timestamp (RFC3339 or 2006-01-02 15:04:05)
      --since=SINCE        Relative start time (e.g., 1h, 30m, 2h30m)
      --[no-]progress      Show progress during message fetching
      --[no-]distribution  Show message distribution over streams
      --[no-]gui           Launch web-based interactive GUI
      --gui-port=8080      Port for web-based GUI server
      --[no-]browser       Auto-open browser when GUI starts
```
## Notes

- 
- Running this can take time as it will when starting try to get every single message from every single limits stream within the specified time interval (one by one , using batched direct gets to try and limit the impact).
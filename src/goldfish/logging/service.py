"""Service for executing LogsQL queries against VictoriaLogs."""

import json
import textwrap

import httpx

from .settings import get_settings

MAX_LINES_RETURNED = 120  # Hard cap to keep LLM context small
TRUNCATE_MSG_AT = 200  # Characters


def _format_log_results(response_text: str) -> str:
    """Format ND-JSON log results into readable output.

    Args:
        response_text: Raw response body from VictoriaLogs

    Returns:
        Formatted log output
    """
    # Parse ND-JSON (newline-delimited JSON)
    lines = [line for line in response_text.splitlines() if line.strip()]
    if not lines:
        return "No results. Ensure your query includes a time filter like _time:5m"

    # Format results
    pretty = [f"## {len(lines):,} log lines returned (showing first {min(len(lines), MAX_LINES_RETURNED)})"]

    for i, raw in enumerate(lines[:MAX_LINES_RETURNED], start=1):
        try:
            log = json.loads(raw)
        except json.JSONDecodeError:
            pretty.append(f"[{i:03}] <invalid JSON line>")
            continue

        # Extract key fields
        ts = log.get("_time", "")[:23]  # Truncate microseconds
        sev = log.get("severity", "").upper()[:5]
        msg = log.get("_msg", "") or log.get("message", "")

        # Truncate long messages
        if len(msg) > TRUNCATE_MSG_AT:
            msg = msg[:TRUNCATE_MSG_AT] + "..."

        # Format main line
        if sev:
            pretty.append(f"[{i:03}] {ts} [{sev}] {msg}")
        else:
            pretty.append(f"[{i:03}] {ts} {msg}")

        # Add labels/fields (excluding internal fields)
        labels = {
            k: v
            for k, v in log.items()
            if k not in {"_time", "_msg", "message", "severity"} and not k.startswith("_stream") and v
        }

        if labels:
            label_parts = []
            for k, v in sorted(labels.items()):
                if isinstance(v, str) and len(v) > 50:
                    v = v[:50] + "..."
                label_parts.append(f"{k}={v}")

            if label_parts:
                wrapped = textwrap.fill(
                    ", ".join(label_parts),
                    width=80,
                    initial_indent="      ",
                    subsequent_indent="      ",
                )
                pretty.append(wrapped)

    if len(lines) > MAX_LINES_RETURNED:
        pretty.append(
            f"\n... {len(lines) - MAX_LINES_RETURNED:,} more lines truncated. "
            f"Use pipes like '| head 20' or '| stats' to control output."
        )

    return "\n".join(pretty)


def search_logs_sync(query: str) -> str:
    """Execute a LogsQL query synchronously and return formatted results.

    Use this from sync contexts (like MCP tools running in an event loop).

    Args:
        query: Raw LogsQL query string

    Returns:
        Formatted search results or error message
    """
    settings = get_settings()

    if not query.strip():
        return "Missing required parameter: `query` (raw LogsQL string)"

    base_url = settings.logging.victoria_logs_url

    try:
        with httpx.Client() as client:
            resp = client.post(
                f"{base_url}/select/logsql/query",
                data={"query": query},
                timeout=30.0,
            )
            resp.raise_for_status()
    except httpx.HTTPError as e:
        return f"Could not reach VictoriaLogs at {base_url}: {e}"

    return _format_log_results(resp.text)


async def search_logs(query: str) -> str:
    """Execute a LogsQL query asynchronously and return formatted results.

    Args:
        query: Raw LogsQL query string

    Returns:
        Formatted search results or error message
    """
    settings = get_settings()

    if not query.strip():
        return "Missing required parameter: `query` (raw LogsQL string)"

    base_url = settings.logging.victoria_logs_url

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{base_url}/select/logsql/query",
                data={"query": query},
                timeout=30.0,
            )
            resp.raise_for_status()
    except httpx.HTTPError as e:
        return f"Could not reach VictoriaLogs at {base_url}: {e}"

    return _format_log_results(resp.text)


# LogsQL quick reference for the tool description
LOGSQL_GUIDE = """
LogsQL pocket guide
-------------------
QUERY CORE - one-liners
  _time:5m error                    # AND implicit, put _time first
  _time:5m error OR warning         # use OR, NOT/-, and () for precedence
  {app="goldfish"} error            # label/stream filter
  status:error                      # field selector (default _msg)

FILTER PATTERNS
  word        : error               # matches "error" anywhere
  phrase      : "disk full"         # exact phrase match
  prefix      : erro*               # prefix matching
  substring   : _msg~"fatal"        # substring search
  exact/multi : status:=500         # exact match
              : status:=(500,503)   # multiple values
  range/cmp   : latency_ms:>500     # comparison operators
  regex       : url:~"/api/.+"      # regular expression

TOP PIPES (|)
  sort by (_time desc)              # sort results
  limit N / head N                  # limit output
  fields f1,f2                      # select fields
  stats count() by (endpoint)       # aggregations
  where latency_ms > 1000           # filter after initial query
  top 10 endpoint                   # top values
  uniq by (user)                    # unique values

STATS FUNCTIONS
  count(), sum(x), avg(x), min/max(x), quantile(0.95)(x),
  histogram(x,bucket), rate(x), count_uniq(x), values(x)

SPEED HINTS
  - Always start with _time:XXX to narrow time range
  - Use {label=value} filters early in query
  - Sort/regex only after initial filters
  - Use sample N for large result sets

Examples
--------
1. Last 20 errors
   _time:30m error | sort by (_time desc) | head 20

2. Goldfish worker logs
   _time:15m {app="goldfish"} {component="worker"} | head 50

3. Per-minute error rate
   _time:1h error | stats by (bucket=_time(1m)) count() errors

4. Search specific project
   _time:2h {project~"/Users/luka"} error
"""

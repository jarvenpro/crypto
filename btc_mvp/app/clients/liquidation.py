from __future__ import annotations

from dataclasses import dataclass

from app.telegram_tools import run_powershell_json


@dataclass(slots=True)
class LiquidationEvent:
    side: str
    price: float
    quantity: float
    notional_usd: float
    trade_time: str


class BinanceLiquidationClient:
    def sample_force_orders(self, symbol: str, sample_seconds: int = 4) -> list[LiquidationEvent]:
        stream_name = f"{symbol.lower()}@forceOrder"
        websocket_url = f"wss://fstream.binance.com/ws/{stream_name}"
        timeout_ms = max(sample_seconds * 1000 + 3000, 8000)

        script = f"""
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$uri = '{websocket_url}'
$durationMs = {sample_seconds * 1000}
$socket = [System.Net.WebSockets.ClientWebSocket]::new()
$cts = [System.Threading.CancellationTokenSource]::new()
$cts.CancelAfter($durationMs)
$buffer = New-Object byte[] 16384
$segment = [ArraySegment[byte]]::new($buffer)
$messages = New-Object System.Collections.Generic.List[string]

try {{
    $socket.ConnectAsync([Uri]$uri, $cts.Token).GetAwaiter().GetResult()
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.ElapsedMilliseconds -lt $durationMs -and $socket.State -eq [System.Net.WebSockets.WebSocketState]::Open) {{
        $receiveTask = $socket.ReceiveAsync($segment, $cts.Token)
        $completed = $receiveTask.Wait([Math]::Max(200, $durationMs - $sw.ElapsedMilliseconds))
        if (-not $completed) {{ continue }}
        $result = $receiveTask.Result
        if ($result.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Close) {{ break }}

        $count = $result.Count
        while (-not $result.EndOfMessage) {{
            $next = $socket.ReceiveAsync($segment, $cts.Token).GetAwaiter().GetResult()
            $count += $next.Count
            $result = $next
        }}

        $text = [System.Text.Encoding]::UTF8.GetString($buffer, 0, $count)
        if ($text) {{ $messages.Add($text) }}
    }}
}} catch {{
}} finally {{
    if ($socket.State -eq [System.Net.WebSockets.WebSocketState]::Open) {{
        $socket.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, 'done', [Threading.CancellationToken]::None).GetAwaiter().GetResult()
    }}
    $socket.Dispose()
    $cts.Dispose()
}}

$payload = @()
foreach ($msg in $messages) {{
    try {{
        $obj = $msg | ConvertFrom-Json
        $o = $obj.o
        if ($null -ne $o) {{
            $price = [double]$o.ap
            if ($price -eq 0) {{ $price = [double]$o.p }}
            $qty = [double]$o.z
            if ($qty -eq 0) {{ $qty = [double]$o.q }}
            $payload += [PSCustomObject]@{{
                side = [string]$o.S
                price = $price
                quantity = $qty
                notional_usd = [Math]::Round($price * $qty, 2)
                trade_time = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$o.T).UtcDateTime.ToString('o')
            }}
        }}
    }} catch {{
    }}
}}
if ($payload.Count -eq 0) {{
    '[]'
}} else {{
    $payload | ConvertTo-Json -Depth 5 -Compress
}}
"""

        try:
            payload = run_powershell_json(script, timeout=timeout_ms)
        except Exception:
            return []
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            return []

        events: list[LiquidationEvent] = []
        for item in payload:
            try:
                events.append(
                    LiquidationEvent(
                        side=str(item["side"]),
                        price=float(item["price"]),
                        quantity=float(item["quantity"]),
                        notional_usd=float(item["notional_usd"]),
                        trade_time=str(item["trade_time"]),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return events

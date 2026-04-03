from __future__ import annotations

import base64
import json
import subprocess


def run_powershell_json(script: str, timeout: int = 30) -> dict:
    encoded_script = _to_encoded_command(script)
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            encoded_script,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "PowerShell command failed.")
    stdout = result.stdout.strip()
    if not stdout:
        raise RuntimeError("PowerShell command returned no response body.")
    return json.loads(stdout)


def telegram_post_via_powershell(url: str, payload: dict) -> dict:
    body_json = json.dumps(payload, ensure_ascii=False)
    body_base64 = base64.b64encode(body_json.encode("utf-8")).decode("ascii")
    script = (
        "[Console]::InputEncoding = [System.Text.Encoding]::UTF8; "
        "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
        f"$payload = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String('{body_base64}')); "
        f"$resp = Invoke-RestMethod -Method Post -Uri '{url}' -ContentType 'application/json; charset=utf-8' -Body $payload; "
        "$resp | ConvertTo-Json -Depth 8 -Compress"
    )
    return run_powershell_json(script, timeout=30)


def telegram_get_via_powershell(url: str) -> dict:
    script = (
        "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
        f"$resp = Invoke-RestMethod -Method Get -Uri '{url}'; "
        "$resp | ConvertTo-Json -Depth 8 -Compress"
    )
    return run_powershell_json(script, timeout=30)


def _to_encoded_command(script: str) -> str:
    return base64.b64encode(script.encode("utf-16le")).decode("ascii")

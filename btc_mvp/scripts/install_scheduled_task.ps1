$ErrorActionPreference = "Stop"

$taskName = "BTC_MVP_BOT"
$projectDir = "D:\crypto\btc_mvp"
$scriptPath = Join-Path $projectDir "scripts\run_bot.ps1"
$startTime = (Get-Date).AddMinutes(1).ToString("HH:mm")
$taskCommand = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""

Write-Host "Creating or updating scheduled task: $taskName"
schtasks /Create /F /SC MINUTE /MO 5 /TN $taskName /TR $taskCommand /ST $startTime | Out-Null

Write-Host "Scheduled task created successfully."
Write-Host "Task name: $taskName"
Write-Host "Run interval: every 5 minutes"
Write-Host "Project path: $projectDir"

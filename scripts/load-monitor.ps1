param([Parameter(Mandatory=$true)][string]$RunDir)
$ErrorActionPreference = 'Stop'
$loadProcessRecords = Get-Content -Raw -Encoding UTF8 (Join-Path $RunDir 'processes.json') | ConvertFrom-Json
$previousCPU = @{}
$previousClock = Get-Date
while (!(Test-Path -LiteralPath (Join-Path $RunDir 'monitor.stop'))) {
    $sampleClock = Get-Date
    $elapsedSecs = ($sampleClock - $previousClock).TotalSeconds
    $sampleRows = foreach ($record in $loadProcessRecords) {
        $actual = Get-Process -Id $record.Id -ErrorAction SilentlyContinue
        if ($actual -and $actual.Path -eq $record.Path) {
            $cpuSecs = $actual.TotalProcessorTime.TotalSeconds
            $cpuPercent = 0
            if ($previousCPU.ContainsKey($record.Id) -and $elapsedSecs -gt 0) {
                $cpuPercent = [math]::Round(100 * ($cpuSecs - $previousCPU[$record.Id]) / $elapsedSecs, 2)
            }
            $previousCPU[$record.Id] = $cpuSecs
            [PSCustomObject]@{Service=$record.Name;CPUPercentOneCore=$cpuPercent;WorkingSetMB=[math]::Round($actual.WorkingSet64/1MB,2);Running=$true}
        } else { [PSCustomObject]@{Service=$record.Name;Running=$false} }
    }
    $previousClock = $sampleClock
    ConvertTo-Json -Compress -InputObject @{At=$sampleClock.ToUniversalTime().ToString('o');Processes=@($sampleRows)} -Depth 4 | Add-Content -Encoding UTF8 (Join-Path $RunDir 'process-resources.jsonl')
    Start-Sleep -Seconds 5
}

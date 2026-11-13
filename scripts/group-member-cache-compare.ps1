param([int]$MessageCount=600)
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
$source=Get-Content -Raw -Encoding UTF8 (Join-Path $PSScriptRoot 'message-push-timing.ps1')
# Adapt the existing bounded workload in a temporary runner. Preserve the
# original experiment and use the same binary/config except cache Disabled.
$required=@(
    '$root = Split-Path -Parent $PSScriptRoot',
    '"im-message-push-timing-$stamp"',
    "if (`$service -eq 'realtime' -and `$trial -ne 'control')",
    '            $yaml +='
)
foreach($text in $required){if(!$source.Contains($text)){throw "Timing harness changed; review cache comparison adapter: $text"}}
$source=$source.Replace('$root = Split-Path -Parent $PSScriptRoot',("`$root = '"+$root.Replace("'","''")+"'"))
$source=$source.Replace('"im-message-push-timing-$stamp"','"im-group-member-cache-compare-$stamp"')
$source=$source.Replace("if (`$service -eq 'realtime' -and `$trial -ne 'control')","if (`$service -eq 'realtime')")
$source=$source.Replace('Current code only; identical config, trace enabled/disabled/enabled; one observer of 101 group members; closed-loop bursts, not sustained capacity',
    'Same current binary; recipient cache enabled/disabled/enabled; tracing enabled in all three trials; one observer of 101 members; closed-loop bursts, not sustained capacity')
$injection=@'
            if ($service -eq 'realtime') {
                $yaml=$yaml.Replace('Channel: im:group-members:invalidate:v1',"Channel: $topic-members")
                if ($trial -eq 'control') {$yaml=$yaml.Replace('Disabled: false','Disabled: true')}
            }
'@
$source=$source.Replace('            $yaml +=',($injection+"`n"+'            $yaml +='))
$runner=Join-Path $env:TEMP ('im-group-member-cache-runner-'+[guid]::NewGuid().ToString('N')+'.ps1')
[IO.File]::WriteAllText($runner,$source,[Text.UTF8Encoding]::new($false))
try {
    & powershell -NoProfile -ExecutionPolicy Bypass -File $runner -MessageCount $MessageCount
    if($LASTEXITCODE -ne 0){throw 'Group member cache comparison failed'}
} finally {Remove-Item -LiteralPath $runner -ErrorAction SilentlyContinue}

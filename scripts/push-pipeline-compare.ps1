param(
    [ValidateSet('Burst','Sustained')][string]$Mode='Burst',
    [string]$BaselineBackup='C:\Users\21201\Downloads\IM\IM\im-push-pipeline-backup-20261109-204320',
    [int]$MessageCount=600,
    [int]$Seconds=20,
    [string]$Rates='100,300,600',
    [string]$Groups='1,20',
    [ValidateRange(1,128)][int]$WriterBatchSize=32,
    [switch]$CurrentOnly,
    [switch]$TraceSustained,
    [switch]$Profile
)
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
if(!(Test-Path -LiteralPath (Join-Path $BaselineBackup 'common/messagepublish/publisher.go'))){throw 'Pre-optimization source backup required'}
$source=Get-Content -Raw -Encoding UTF8 (Join-Path $PSScriptRoot 'message-push-timing.ps1')
$order=if($Mode -eq 'Burst'){"@('baseline-r1','current-r1','current-r2','baseline-r2')"}else{"@('baseline-r1','current-r1')"}
$count=if($Mode -eq 'Burst'){4}else{2}
if($CurrentOnly){$order="@('current-r1')";$count=1}
$source=$source.Replace('$root = Split-Path -Parent $PSScriptRoot',("`$root = '"+$root.Replace("'","''")+"'"))
$source=$source.Replace('"im-message-push-timing-$stamp"',('"im-push-pipeline-'+$Mode.ToLower()+'-$stamp"'))
$source=$source.Replace("Order=@('trace-r1','control','trace-r2')",("Order="+$order))
$source=$source.Replace('Current code only; identical config, trace enabled/disabled/enabled; one observer of 101 group members; closed-loop bursts, not sustained capacity',
    ('Matched pre-optimization source vs current; mode='+$Mode+'; identical configuration and infrastructure; group recipient cache enabled in both'))
$source=$source.Replace('for ($trialNumber=1; $trialNumber -le 3; $trialNumber++) {',("for (`$trialNumber=1; `$trialNumber -le $count; `$trialNumber++) {"))
$source=$source.Replace("    `$trial = @('trace-r1','control','trace-r2')[`$trialNumber-1]",("    `$trial = "+$order+"[`$trialNumber-1]`n    `$variant=if(`$trial.StartsWith('baseline')){'baseline'}else{'current'}"))
$source=$source.Replace('im_batch_perf_${stamp}_current_$trialNumber','im_batch_perf_${stamp}_${variant}_$trialNumber')
$source=$source.Replace("if (`$service -eq 'realtime' -and `$trial -ne 'control')","if (`$service -eq 'realtime' -and (`$env:IM_PIPELINE_LOAD_MODE -eq 'Burst' -or `$env:IM_PIPELINE_TRACE_SUSTAINED -eq '1'))")
$source=$source.Replace("if (`$trial -ne 'control' -and !(Test-Path", "if ((`$env:IM_PIPELINE_LOAD_MODE -eq 'Burst' -or `$env:IM_PIPELINE_TRACE_SUSTAINED -eq '1') -and !(Test-Path")
$source=$source.Replace("`$binary = if (`$service -eq 'realtime') {'current'} else {`$service}","`$binary = if (`$service -eq 'realtime') {`$variant} else {`$service}")
$source=$source.Replace("`$env:IM_BATCH_PERF_VARIANT = 'current'","`$env:IM_BATCH_PERF_VARIANT = `$variant")
$source=$source.Replace('        node $driverPath','        node scripts/push-pipeline-load.mjs')
$source=$source.Replace('            $yaml +=',('            $yaml=$yaml.Replace(''Channel: im:group-members:invalidate:v1'',"Channel: $topic-members")'+"`n"+'            $chosenSize=if($variant -eq ''current''){$env:IM_PIPELINE_WRITER_BATCH_SIZE}else{''32''}; $yaml=[regex]::Replace($yaml,''(?ms)(  Batching:.*?    BatchSize: )\d+'',(''${1}''+$chosenSize))'+"`n"+'            $yaml +='))
$source=$source.Replace('RunID=$stamp;', 'CurrentWriterBatchSize=[int]$env:IM_PIPELINE_WRITER_BATCH_SIZE; BaselineWriterBatchSize=32; RunID=$stamp;')
$overlayBuild=@'
$replacements=@{}
foreach($relative in @('common/messagepublish','service/message','service/realtime')){
    foreach($file in Get-ChildItem -LiteralPath (Join-Path $root $relative) -Recurse -File -Filter '*.go'){
        $relativeFile=$file.FullName.Substring($root.Length+1)
        $archived=Join-Path $env:IM_PIPELINE_BACKUP $relativeFile
        $replacements[$file.FullName]=if(Test-Path -LiteralPath $archived){$archived}else{''}
    }
}
$overlay=Join-Path $runDir 'baseline-overlay.json'
Write-Utf8 $overlay (@{Replace=$replacements}|ConvertTo-Json -Depth 4)
Write-Host 'Build baseline'
go build -overlay $overlay -o (Join-Path $runDir 'baseline.exe') ./service/realtime
if($LASTEXITCODE -ne 0){throw 'Baseline build failed'}
if($env:IM_PIPELINE_PROFILE -eq '1'){
    if(Get-NetTCPConnection -State Listen -LocalPort 19060 -ErrorAction SilentlyContinue){throw 'Diagnostic pprof port occupied'}
    $mainPath=Join-Path $root 'service/realtime/realtime.go'
    $profileMain=Get-Content -Raw -Encoding UTF8 $mainPath
    $profileMain=$profileMain.Replace('"time"',('"time"'+"`n"+'"net/http"'+"`n"+'_ "net/http/pprof"'))
    $profileMain=$profileMain.Replace('flag.Parse()',('flag.Parse()'+"`n"+'go func(){log.Printf("diagnostic profiler stopped: %v",http.ListenAndServe("127.0.0.1:19060",nil))}()'))
    $profilePath=Join-Path $runDir 'diagnostic-realtime.go'
    Write-Utf8 $profilePath $profileMain
    $profileOverlay=Join-Path $runDir 'diagnostic-overlay.json'
    Write-Utf8 $profileOverlay (@{Replace=@{$mainPath=$profilePath}}|ConvertTo-Json -Depth 4)
    go build -overlay $profileOverlay -o (Join-Path $runDir 'current.exe') ./service/realtime
    if($LASTEXITCODE -ne 0){throw 'Diagnostic binary build failed'}
}
'@
$source=$source.Replace('function Invoke-TestSql([string]$query) {',($overlayBuild+"`n"+'function Invoke-TestSql([string]$query) {'))
$runner=Join-Path $env:TEMP ('im-push-pipeline-runner-'+[guid]::NewGuid().ToString('N')+'.ps1')
[IO.File]::WriteAllText($runner,$source,[Text.UTF8Encoding]::new($false))
$env:IM_PIPELINE_LOAD_MODE=$Mode
$env:IM_PIPELINE_DURATION_SECONDS=[string]$Seconds
$env:IM_PIPELINE_BACKUP=$BaselineBackup
$env:IM_PIPELINE_RATES=$Rates
$env:IM_PIPELINE_GROUPS=$Groups
$env:IM_PIPELINE_WRITER_BATCH_SIZE=[string]$WriterBatchSize
$env:IM_PIPELINE_TRACE_SUSTAINED=if($TraceSustained){'1'}else{'0'}
$env:IM_PIPELINE_PROFILE=if($Profile){'1'}else{'0'}
try{
    & powershell -NoProfile -ExecutionPolicy Bypass -File $runner -MessageCount $MessageCount
    if($LASTEXITCODE -ne 0){throw 'Pipeline comparison failed'}
}finally{
    Remove-Item -LiteralPath $runner -ErrorAction SilentlyContinue
    Remove-Item Env:IM_PIPELINE_LOAD_MODE,Env:IM_PIPELINE_DURATION_SECONDS,Env:IM_PIPELINE_BACKUP,Env:IM_PIPELINE_RATES,Env:IM_PIPELINE_GROUPS,Env:IM_PIPELINE_WRITER_BATCH_SIZE,Env:IM_PIPELINE_TRACE_SUSTAINED,Env:IM_PIPELINE_PROFILE -ErrorAction SilentlyContinue
}

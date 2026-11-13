param(
    [string]$BaselineBackup = 'C:\Users\21201\Downloads\IM\IM\im-seq-batch-implementation-backup-20261109-164931',
    [int]$MessageCount = 600,
    [int]$Rounds = 2
)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if ($MessageCount -lt 100 -or $MessageCount -gt 5000 -or $Rounds -lt 1 -or $Rounds -gt 3) { throw 'Invalid bounded comparison settings' }
if (!(Test-Path -LiteralPath "$BaselineBackup/service/realtime/internal/svc/servicecontext.go")) { throw 'The pre-batch source backup is required' }
$stamp=Get-Date -Format 'yyyyMMdd_HHmmss'
$runDir=Join-Path $env:TEMP "im-message-batch-compare-$stamp"
New-Item -ItemType Directory -Path $runDir | Out-Null
Write-Host "Comparison artifacts: $runDir"
$occupied=Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object LocalPort -In @(18080,18105,19101)
if ($occupied) { throw 'Performance test ports 18080/18105/19101 are occupied' }

# Build the real pre-batch source using a Go overlay, without reverting any
# working-tree files. A blank replacement removes newly introduced Go files.
$replacements=@{}
foreach($relative in @('service/message','service/realtime')) {
    foreach($file in Get-ChildItem -LiteralPath (Join-Path $root $relative) -Recurse -File -Filter '*.go') {
        $path=$file.FullName.Substring($root.Length+1)
        $archived=Join-Path $BaselineBackup $path
        if (Test-Path -LiteralPath $archived) {$replacements[$file.FullName]=$archived} else {$replacements[$file.FullName]=''}
    }
}
$overlay=Join-Path $runDir 'baseline-overlay.json'
[IO.File]::WriteAllText($overlay,(@{Replace=$replacements}|ConvertTo-Json -Depth 4),[Text.UTF8Encoding]::new($false))
$builds=@(
    @{Name='rpc'; Package='./service/user/rpc'},
    @{Name='offline'; Package='./service/offline'},
    @{Name='fixtures'; Package='./scripts/message-batch-fixtures'},
    @{Name='current'; Package='./service/realtime'},
    @{Name='baseline'; Package='./service/realtime'}
)
foreach($build in $builds) {
    Write-Host "Build $($build.Name)"
    if($build.Name -eq 'baseline') {go build -overlay $overlay -o (Join-Path $runDir 'baseline.exe') $build.Package}
    else {go build -o (Join-Path $runDir "$($build.Name).exe") $build.Package}
    if($LASTEXITCODE -ne 0) {throw "Build failed: $($build.Name)"}
}
function Invoke-TestSql([string]$query) {
    $result=$query | docker exec -i im-learn-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --raw --skip-column-names'
    if($LASTEXITCODE -ne 0) {throw 'Disposable comparison database setup failed'}
    return $result
}
$environment=@{RunID=$stamp;MessageCount=$MessageCount;Rounds=$Rounds;BaselineBackup=$BaselineBackup;KafkaPartitions=3;KafkaReplicationFactor=1;RealtimePool=24;RPCPool=16;OfflinePool=16;Go=(go version);Node=(node --version);Docker=(docker version --format '{{.Server.Version}}');Method='Alternating pre-batch and current binaries, same host, full WS/RPC/MySQL/Kafka/WS path, login setup outside measurements'}
$environment.MySQLSettings=@(Invoke-TestSql "SHOW VARIABLES WHERE Variable_name IN ('innodb_flush_log_at_trx_commit','sync_binlog','max_connections','transaction_isolation');")
$environment.DockerResources=docker info --format '{{.NCPU}} CPU / {{.MemTotal}} bytes'
$environment.HostCPU=@(Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors)
[IO.File]::WriteAllText((Join-Path $runDir 'environment.json'),($environment|ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))

for($round=1;$round -le $Rounds;$round++) {
    $order=if($round%2 -eq 1){@('baseline','current')}else{@('current','baseline')}
    foreach($variant in $order) {
        $trial="$variant-r$round"
        $trialDir=Join-Path $runDir $trial
        New-Item -ItemType Directory -Path $trialDir | Out-Null
        $database="im_batch_perf_${stamp}_${variant}_$round"
        $topic="im-batch-perf-$stamp-$variant-$round"
        $registry="batchperf.user.rpc.$stamp.$variant.$round"
        $processes=@()
        try {
            Write-Host "SETUP $trial"
            $null=Invoke-TestSql "CREATE DATABASE $database CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
            foreach($table in @('user','session','user_session','friend','apply_friend','user_balance','balance_log','conversation_sequence','message')) {
                $ddl=(Get-Content -Raw -Encoding UTF8 "sql/$table.sql").Replace('USE `infiniteChat`;',"USE $database;")
                $null=Invoke-TestSql $ddl
            }
            docker exec im-learn-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions 3 --replication-factor 1 | Out-Null
            if($LASTEXITCODE -ne 0){throw 'Cannot create isolated Kafka topic'}
            $password=docker exec im-learn-mysql printenv MYSQL_ROOT_PASSWORD
            $dsn="root:${password}@tcp(127.0.0.1:3306)/${database}?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai"
            $secret=[Guid]::NewGuid().ToString('N')+[Guid]::NewGuid().ToString('N')
            $env:IM_BATCH_PERF_DSN=$dsn
            $env:IM_BATCH_PERF_SECRET=$secret
            & (Join-Path $runDir 'fixtures.exe') -output (Join-Path $trialDir 'fixtures.json')
            if($LASTEXITCODE -ne 0){throw 'Fixture setup failed'}
            foreach($service in @('rpc','offline','realtime')) {
                $source=switch($service){'rpc' {'service/user/rpc/etc/user.example.yaml'} 'offline' {'service/offline/etc/offline.example.yaml'} 'realtime' {'service/realtime/etc/realtime.example.yaml'}}
                $yaml=Get-Content -Raw -Encoding UTF8 $source
                $yaml=[regex]::Replace($yaml,'(?m)^DataSource:.*$',"DataSource: $dsn")
                $yaml=[regex]::Replace($yaml,'(?m)^(\s*)AccessSecret:.*$',"`$1AccessSecret: $secret")
                $yaml=[regex]::Replace($yaml,'(?m)^(\s*)Key: user\.rpc\s*$',"`$1Key: $registry")
                $yaml=$yaml.Replace('0.0.0.0:8080','127.0.0.1:18080').Replace('0.0.0.0:9101','127.0.0.1:19101').Replace('Port: 8105','Port: 18105')
                $yaml=$yaml.Replace('message-persisted-topic',$topic).Replace('message-consumer-dlq-topic',"$topic-dlq")
                $yaml=[regex]::Replace($yaml,'(?m)^(\s*(?:MessageWriterGroup|PushGroup|MessageCacheGroup):\s*)([^\r\n]+)',"`$1`$2-$stamp-$variant-$round")
                # Both binaries receive identical settings; the baseline parser
                # ignores the newly added optional batching block.
                $yaml+="`nLog:`n  Mode: console`n  Level: error`n  Stat: false`n"
                [IO.File]::WriteAllText((Join-Path $trialDir "$service.yaml"),$yaml,[Text.UTF8Encoding]::new($false))
                $binary=if($service -eq 'realtime'){$variant}else{$service}
                $processes+=Start-Process -FilePath (Join-Path $runDir "$binary.exe") -ArgumentList @('-f',(Join-Path $trialDir "$service.yaml")) -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $trialDir "$service.stdout.log") -RedirectStandardError (Join-Path $trialDir "$service.stderr.log")
            }
            $env:IM_BATCH_PERF_TRIAL=$trialDir
            $env:IM_BATCH_PERF_DB=$database
            $env:IM_BATCH_PERF_VARIANT=$variant
            $env:IM_BATCH_PERF_MESSAGES=[string]$MessageCount
            node scripts/message-batch-compare.mjs
            if($LASTEXITCODE -ne 0){throw "Performance trial failed: $trial"}
            $drops=Select-String -LiteralPath (Join-Path $trialDir 'realtime.stderr.log') -Pattern 'publish queue full|publish failed' -ErrorAction SilentlyContinue
            Write-Host "FINISHED $trial; publish failure/full log lines: $(@($drops).Count)"
        } finally {
            foreach($process in $processes) {
                $actual=Get-Process -Id $process.Id -ErrorAction SilentlyContinue
                if($actual -and $actual.Path -like "$runDir\*"){Stop-Process -Id $process.Id -ErrorAction SilentlyContinue}
            }
            Remove-Item Env:IM_BATCH_PERF_DSN,Env:IM_BATCH_PERF_SECRET -ErrorAction SilentlyContinue
        }
    }
}
Write-Host "Comparison complete: $runDir"

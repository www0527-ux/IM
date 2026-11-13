param([int]$MessageCount = 600)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if ($MessageCount -lt 100 -or $MessageCount -gt 1000) { throw 'Timing run supports 100..1000 messages per case' }
if (Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object LocalPort -In @(18080,18105,19101)) {
    throw 'Timing test ports 18080/18105/19101 are occupied'
}
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$runDir = Join-Path $env:TEMP "im-message-push-timing-$stamp"
New-Item -ItemType Directory -Path $runDir | Out-Null
Write-Host "Timing artifacts: $runDir"
function Write-Utf8([string]$path, [string]$value) {
    [IO.File]::WriteAllText($path, $value, [Text.UTF8Encoding]::new($false))
}

# Reuse the existing validated workload without editing experimental sources.
# Reduce to four cases and export only per-message IDs and observed latencies.
$driver = Get-Content -Raw -Encoding UTF8 scripts/message-batch-compare.mjs
if (!$driver.Contains('[10, 50, 100]') -or !$driver.Contains('  for (const socket of sockets) socket.close();')) {
    throw 'Workload source changed; review this timing adapter'
}
$driver = $driver.Replace('[10, 50, 100]', '[10, 100]')
$driver = $driver.Replace('  for (const socket of sockets) socket.close();', @'
  writeFileSync(join(runDir, 'message-observations.json'), JSON.stringify(
    [...tracked.values()].map(({ id, group, phase, ok, ackMs, pushMs }) => ({ id, group, phase, ok, ackMs, pushMs }))));
  await delay(1800);
  for (const socket of sockets) socket.close();
'@)
$driverPath = Join-Path $runDir 'timing-driver.mjs'
Write-Utf8 $driverPath $driver
foreach ($build in @(
    @{Name='rpc'; Package='./service/user/rpc'},
    @{Name='offline'; Package='./service/offline'},
    @{Name='fixtures'; Package='./scripts/message-batch-fixtures'},
    @{Name='current'; Package='./service/realtime'}
)) {
    Write-Host "Build $($build.Name)"
    go build -o (Join-Path $runDir "$($build.Name).exe") $build.Package
    if ($LASTEXITCODE -ne 0) { throw "Build failed: $($build.Name)" }
}
function Invoke-TestSql([string]$query) {
    $result = $query | docker exec -i im-learn-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --raw --skip-column-names'
    if ($LASTEXITCODE -ne 0) { throw 'Disposable timing database setup failed' }
    return $result
}
$environment = @{
    RunID=$stamp; MessageCount=$MessageCount; Order=@('trace-r1','control','trace-r2');
    KafkaPartitions=3; KafkaReplicationFactor=1; RealtimePool=24; RPCPool=16; OfflinePool=16;
    Go=(go version); Node=(node --version); Docker=(docker version --format '{{.Server.Version}}');
    Method='Current code only; identical config, trace enabled/disabled/enabled; one observer of 101 group members; closed-loop bursts, not sustained capacity';
    MySQLSettings=@(Invoke-TestSql "SHOW VARIABLES WHERE Variable_name IN ('innodb_flush_log_at_trx_commit','sync_binlog','max_connections','transaction_isolation');");
    DockerResources=(docker info --format '{{.NCPU}} CPU / {{.MemTotal}} bytes')
}
Write-Utf8 (Join-Path $runDir 'environment.json') ($environment | ConvertTo-Json -Depth 5)
for ($trialNumber=1; $trialNumber -le 3; $trialNumber++) {
    $trial = @('trace-r1','control','trace-r2')[$trialNumber-1]
    $trialDir = Join-Path $runDir $trial
    New-Item -ItemType Directory -Path $trialDir | Out-Null
    $database = "im_batch_perf_${stamp}_current_$trialNumber"
    $topic = "im-push-timing-$stamp-$trialNumber"
    $registry = "pushtiming.user.rpc.$stamp.$trialNumber"
    $processes = @()
    try {
        Write-Host "SETUP $trial"
        $null = Invoke-TestSql "CREATE DATABASE $database CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
        foreach ($table in @('user','session','user_session','friend','apply_friend','user_balance','balance_log','conversation_sequence','message')) {
            $ddl = (Get-Content -Raw -Encoding UTF8 "sql/$table.sql").Replace('USE `infiniteChat`;', "USE $database;")
            $null = Invoke-TestSql $ddl
        }
        docker exec im-learn-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions 3 --replication-factor 1 | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'Cannot create isolated Kafka topic' }
        $password = docker exec im-learn-mysql printenv MYSQL_ROOT_PASSWORD
        $dsn = "root:${password}@tcp(127.0.0.1:3306)/${database}?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai"
        $secret = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
        $env:IM_BATCH_PERF_DSN = $dsn
        $env:IM_BATCH_PERF_SECRET = $secret
        & (Join-Path $runDir 'fixtures.exe') -output (Join-Path $trialDir 'fixtures.json')
        if ($LASTEXITCODE -ne 0) { throw 'Fixture setup failed' }
        $fixture = Get-Content -Raw -Encoding UTF8 (Join-Path $trialDir 'fixtures.json') | ConvertFrom-Json
        foreach ($service in @('rpc','offline','realtime')) {
            $source = switch ($service) {
                'rpc' {'service/user/rpc/etc/user.example.yaml'}
                'offline' {'service/offline/etc/offline.example.yaml'}
                'realtime' {'service/realtime/etc/realtime.example.yaml'}
            }
            $yaml = Get-Content -Raw -Encoding UTF8 $source
            $yaml = [regex]::Replace($yaml, '(?m)^DataSource:.*$', "DataSource: $dsn")
            $yaml = [regex]::Replace($yaml, '(?m)^(\s*)AccessSecret:.*$', "`$1AccessSecret: $secret")
            $yaml = [regex]::Replace($yaml, '(?m)^(\s*)Key: user\.rpc\s*$', "`$1Key: $registry")
            $yaml = $yaml.Replace('0.0.0.0:8080','127.0.0.1:18080').Replace('0.0.0.0:9101','127.0.0.1:19101').Replace('Port: 8105','Port: 18105')
            $yaml = $yaml.Replace('message-persisted-topic',$topic).Replace('message-consumer-dlq-topic',"$topic-dlq")
            $yaml = [regex]::Replace($yaml, '(?m)^(\s*(?:MessageWriterGroup|PushGroup|MessageCacheGroup):\s*)([^\r\n]+)', "`$1`$2-timing-$stamp-$trialNumber")
            $yaml += "`nLog:`n  Mode: console`n  Level: error`n  Stat: false`n"
            Write-Utf8 (Join-Path $trialDir "$service.yaml") $yaml
            if ($service -eq 'realtime' -and $trial -ne 'control') {
                $env:IM_MESSAGE_TRACE_FILE = Join-Path $trialDir 'server-timings.jsonl'
                $env:IM_MESSAGE_TRACE_USER_ID = [string]$fixture.users[100].id
            }
            $binary = if ($service -eq 'realtime') {'current'} else {$service}
            $processes += Start-Process -FilePath (Join-Path $runDir "$binary.exe") -ArgumentList @('-f',(Join-Path $trialDir "$service.yaml")) -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $trialDir "$service.stdout.log") -RedirectStandardError (Join-Path $trialDir "$service.stderr.log")
            Remove-Item Env:IM_MESSAGE_TRACE_FILE,Env:IM_MESSAGE_TRACE_USER_ID -ErrorAction SilentlyContinue
        }
        $env:IM_BATCH_PERF_TRIAL = $trialDir
        $env:IM_BATCH_PERF_DB = $database
        $env:IM_BATCH_PERF_VARIANT = 'current'
        $env:IM_BATCH_PERF_MESSAGES = [string]$MessageCount
        node $driverPath
        if ($LASTEXITCODE -ne 0) { throw "Timing trial failed: $trial" }
        $failures = @(Select-String -LiteralPath (Join-Path $trialDir 'realtime.stderr.log') -Pattern 'publish queue full|publish failed|retry|message tracing disabled|message trace flush failed' -ErrorAction SilentlyContinue)
        Write-Host "FINISHED $trial; failure/retry/trace error log lines: $($failures.Count)"
        if ($trial -ne 'control' -and !(Test-Path -LiteralPath (Join-Path $trialDir 'server-timings.jsonl'))) { throw 'Trace file missing' }
    } finally {
        foreach ($process in $processes) {
            $actual = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
            if ($actual -and $actual.Path -like "$runDir\*") { Stop-Process -Id $process.Id -ErrorAction SilentlyContinue }
        }
        Remove-Item Env:IM_MESSAGE_TRACE_FILE,Env:IM_MESSAGE_TRACE_USER_ID,Env:IM_BATCH_PERF_DSN,Env:IM_BATCH_PERF_SECRET -ErrorAction SilentlyContinue
    }
}
Write-Host "Timing complete: $runDir"

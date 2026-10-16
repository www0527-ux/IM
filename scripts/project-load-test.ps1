param([int]$StageSeconds = 12, [int]$MessageLimit = 600, [int]$ClaimRounds = 6, [switch]$IsolateStages, [switch]$RelayTimings, [switch]$WithoutRelayIndex, [switch]$KeepRunning)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$runID = Get-Date -Format 'yyyyMMdd_HHmmss'
$loadRunDir = Join-Path $env:TEMP "im-load-$runID"
$null = New-Item -ItemType Directory -Path $loadRunDir
$loadDatabase = "im_acceptance_$runID"
$ownedProcesses = @()
function Invoke-LoadSql([string]$query) {
    $query | docker exec -i im-learn-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --raw --skip-column-names'
    if ($LASTEXITCODE -ne 0) { throw 'Load database setup failed' }
}
try {
    $ports = @(8080,8103,8104,8105,9101,10010)
    if (Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object LocalPort -In $ports) {
        throw 'Business ports occupied; stop your previous run before load testing'
    }
    Write-Host "Load run: $runID; artifacts: $loadRunDir"
    docker compose up -d mysql redis etcd kafka minio | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Infrastructure startup failed' }
    Invoke-LoadSql "CREATE DATABASE $loadDatabase CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
    foreach ($table in @('user','user_balance','balance_log','session','user_session','friend','apply_friend','conversation_sequence','message','system_notification','red_packet','red_packet_receive','red_packet_share','outbox_event')) {
        $ddl = (Get-Content -Raw -Encoding UTF8 "sql/$table.sql").Replace('USE `infiniteChat`;', "USE $loadDatabase;")
        if ($table -eq 'outbox_event' -and $WithoutRelayIndex) {
            $ddl = [regex]::Replace($ddl, ',\s*INDEX `idx_relay_predecessor`[^\r\n]+', '')
        }
        $null = Invoke-LoadSql $ddl
    }
    $loadSettings = Invoke-LoadSql "SHOW VARIABLES WHERE Variable_name IN ('max_connections','innodb_flush_log_at_trx_commit','sync_binlog','innodb_buffer_pool_size','transaction_isolation');"
    [IO.File]::WriteAllText((Join-Path $loadRunDir 'mysql-settings.tsv'), ($loadSettings -join "`n"), [Text.UTF8Encoding]::new($false))
    $loadPassword = docker exec im-learn-mysql printenv MYSQL_ROOT_PASSWORD
    $loadDSN = "root:${loadPassword}@tcp(127.0.0.1:3306)/${loadDatabase}?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai"
    $loadSecret = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
    $topics = @('message-create-requested-topic','message-persisted-topic','redpacket-cache-build-topic','message-consumer-dlq-topic')
    foreach ($topic in $topics) {
        docker exec im-learn-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$topic-load-$runID" --partitions 3 --replication-factor 1
        if ($LASTEXITCODE -ne 0) { throw 'Topic setup failed' }
    }
    $services = @(
        @{Name='rpc';Package='./service/user/rpc';Config='service/user/rpc/etc/user.example.yaml'},
        @{Name='user';Package='./service/user/api';Config='service/user/api/etc/user-api.example.yaml'},
        @{Name='offline';Package='./service/offline';Config='service/offline/etc/offline.example.yaml'},
        @{Name='realtime';Package='./service/realtime';Config='service/realtime/etc/realtime.example.yaml'},
        @{Name='redpacket';Package='./service/redpacket/api';Config='service/redpacket/api/etc/redpacket-api.example.yaml'},
        @{Name='worker';Package='./service/redpacket/worker';Config='service/redpacket/worker/etc/redpacket-worker.example.yaml'},
        @{Name='relay';Package='./service/outboxrelay';Config='service/outboxrelay/etc/outbox-relay.example.yaml'},
        @{Name='gateway';Package='./service/gateway';Config='service/gateway/etc/gateway.example.yaml'}
    )
    $loadPoolBudgets = @{}
    foreach ($service in $services) {
        Write-Host "Build $($service.Name)"
        go build -o (Join-Path $loadRunDir "$($service.Name).exe") $service.Package
        if ($LASTEXITCODE -ne 0) { throw 'Service build failed' }
        $yaml = Get-Content -Raw -Encoding UTF8 $service.Config
        if ($service.Name -eq 'relay' -and $RelayTimings) {
            $yaml = [regex]::Replace($yaml, '(?m)^\s+LogTimings:.*$', '  LogTimings: true')
        }
        $loadPoolLimit = [regex]::Match($yaml, '(?m)^\s+MaxOpenConns:\s*(\d+)')
        if ($loadPoolLimit.Success) { $loadPoolBudgets[$service.Name] = [int]$loadPoolLimit.Groups[1].Value }
        $yaml = [regex]::Replace($yaml, '(?m)^DataSource:.*$', "DataSource: $loadDSN")
        $yaml = [regex]::Replace($yaml, '(?m)^(\s*)AccessSecret:.*$', "`$1AccessSecret: $loadSecret")
        $yaml = [regex]::Replace($yaml, '(?m)^(\s*)Key: user\.rpc\s*$', "`$1Key: load.user.rpc.$runID")
        if ($service.Name -eq 'user') {
            $loadMinioUser = docker exec im-learn-minio printenv MINIO_ROOT_USER
            $loadMinioPassword = docker exec im-learn-minio printenv MINIO_ROOT_PASSWORD
            $yaml = [regex]::Replace($yaml, '(?m)^(\s*)AccessKey:.*$', "`$1AccessKey: $loadMinioUser")
            $yaml = [regex]::Replace($yaml, '(?m)^(\s*)SecretKey:.*$', "`$1SecretKey: $loadMinioPassword")
        }
        foreach ($topic in $topics) { $yaml = $yaml.Replace($topic, "$topic-load-$runID") }
        $yaml = [regex]::Replace($yaml, '(?m)^(\s*(?:MessageWriterGroup|PushGroup|MessageCacheGroup|CacheBuildGroup):\s*)([^\r\n]+)', "`$1`$2-load-$runID")
        [IO.File]::WriteAllText((Join-Path $loadRunDir "$($service.Name).yaml"), $yaml, [Text.UTF8Encoding]::new($false))
    }
    go build -o (Join-Path $loadRunDir 'helper.exe') ./scripts/acceptance-tools
    if ($LASTEXITCODE -ne 0) { throw 'Audit helper build failed' }
    $loadCPU = Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors
    $loadRAM = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
    $loadDocker = docker info --format '{{json .}}' | ConvertFrom-Json
    $relayLoadYAML = Get-Content -Raw -Encoding UTF8 service/outboxrelay/etc/outbox-relay.example.yaml
    $relayLoadBatch = [int][regex]::Match($relayLoadYAML, '(?m)^\s+BatchSize:\s*(\d+)').Groups[1].Value
    $relayLoadCheckpoint = [int][regex]::Match($relayLoadYAML, '(?m)^\s+CheckpointSize:\s*(\d+)').Groups[1].Value
    $loadEnvironment = @{RunID=$runID;Database=$loadDatabase;CPU=$loadCPU;MemoryBytes=$loadRAM;DockerCPU=$loadDocker.NCPU;DockerMemoryBytes=$loadDocker.MemTotal;DockerVersion=$loadDocker.ServerVersion;Go=(go version);Node=(node --version);StageSeconds=$StageSeconds;MessageLimit=$MessageLimit;ClaimRounds=$ClaimRounds;RelayPollMillis=200;RelayBatchSize=$relayLoadBatch;RelayCheckpointSize=$relayLoadCheckpoint;KafkaPartitions=3;KafkaReplicationFactor=1;ServicesMode='dev, example configuration'}
    $loadEnvironment.MySQLPoolBudgets = $loadPoolBudgets
    $loadEnvironment.IsolateStages = $IsolateStages.IsPresent
    $loadEnvironment.RelayTimings = $RelayTimings.IsPresent
    $loadEnvironment.WithoutRelayIndex = $WithoutRelayIndex.IsPresent
    ConvertTo-Json -InputObject $loadEnvironment -Depth 5 | Set-Content -Encoding UTF8 (Join-Path $loadRunDir 'environment.json')
    $env:IM_MYSQL_DSN = $loadDSN
    $env:IM_LOAD_DB = $loadDatabase
    $env:IM_LOAD_RUN_DIR = $loadRunDir
    $env:IM_LOAD_RUN_ID = $runID
    $env:IM_LOAD_STAGE_SECONDS = [string]$StageSeconds
    $env:IM_LOAD_MESSAGE_LIMIT = [string]$MessageLimit
    $env:IM_LOAD_CLAIM_ROUNDS = [string]$ClaimRounds
    $env:IM_LOAD_ISOLATE_STAGES = [string][int]$IsolateStages.IsPresent
    node scripts/project-load-test.mjs --seed
    if ($LASTEXITCODE -ne 0) { throw 'Load fixtures failed' }
    foreach ($service in $services) {
        $loadArguments = @('-f', (Join-Path $loadRunDir "$($service.Name).yaml"))
        if ($service.Name -eq 'gateway') { $loadArguments += @('-static', (Resolve-Path ../webark-im-template/dist/demo).Path) }
        $ownedProcesses += Start-Process -FilePath (Join-Path $loadRunDir "$($service.Name).exe") -ArgumentList $loadArguments -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $loadRunDir "$($service.Name).stdout.log") -RedirectStandardError (Join-Path $loadRunDir "$($service.Name).stderr.log")
    }
    $records = foreach ($process in $ownedProcesses) { [PSCustomObject]@{Id=$process.Id;Name=$process.ProcessName;Path=$process.Path} }
    ConvertTo-Json -InputObject @($records) -Depth 4 | Set-Content -Encoding UTF8 (Join-Path $loadRunDir 'processes.json')
    node scripts/project-load-test.mjs
    if ($LASTEXITCODE -ne 0) { throw 'Load test ended with a failure; inspect results.json and logs' }
    Write-Host "Load test finished: $loadRunDir/results.json"
} finally {
    if (!$KeepRunning) {
        foreach ($process in $ownedProcesses) {
            $loadActualProcess = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
            if ($loadActualProcess -and $loadActualProcess.Path -eq (Join-Path $loadRunDir "$($process.ProcessName).exe")) {
                Stop-Process -Id $process.Id -ErrorAction SilentlyContinue
            }
        }
    }
    Remove-Item Env:IM_MYSQL_DSN -ErrorAction SilentlyContinue
}

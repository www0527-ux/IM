param([switch]$KeepRunning, [switch]$SkipChecks)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Text.UTF8Encoding]::new($false)
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$runID = Get-Date -Format 'yyyyMMdd_HHmmss'
$runDir = Join-Path $env:TEMP "im-acceptance-$runID"
$null = New-Item -ItemType Directory -Path $runDir
$database = "im_acceptance_$runID"
$processes = @()

function Invoke-TestSql([string]$sql) {
    $sql | docker exec -i im-learn-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --raw --skip-column-names'
    if ($LASTEXITCODE -ne 0) { throw 'Acceptance database setup failed' }
}

try {
    $ports = @(8080,8103,8104,8105,9101,10010,42173)
    $busy = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $_.LocalPort -in $ports }
    if ($busy) { throw 'Acceptance ports are occupied; stop the existing business services first' }
    Write-Host "Acceptance run: $runID"
    Write-Host "Artifacts: $runDir"
    docker info --format '{{.ServerVersion}}' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Start Docker Desktop before running acceptance' }
    if (!$SkipChecks) {
        go test ./...
        if ($LASTEXITCODE -ne 0) { throw 'Go tests failed' }
        go build ./...
        if ($LASTEXITCODE -ne 0) { throw 'Go build failed' }
        Push-Location ../webark-im-template
        try {
            node node_modules/typescript/bin/tsc --noEmit
            if ($LASTEXITCODE -ne 0) { throw 'Frontend type check failed' }
            node node_modules/vite/bin/vite.js build
            if ($LASTEXITCODE -ne 0) { throw 'Frontend production build failed' }
        } finally { Pop-Location }
    }
    docker compose up -d mysql redis etcd kafka kafka-init minio minio-init
    if ($LASTEXITCODE -ne 0) { throw 'Cannot start acceptance infrastructure' }
    Invoke-TestSql "CREATE DATABASE $database CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
    foreach ($table in @('user','user_balance','balance_log','session','user_session','friend','apply_friend','conversation_sequence','message','system_notification','red_packet','red_packet_receive','red_packet_share','outbox_event')) {
        $sql = Get-Content -Encoding utf8 -Raw "sql/$table.sql"
        $sql = $sql.Replace('USE `infiniteChat`;', "USE $database;")
        $null = Invoke-TestSql $sql
    }
    $mysqlPassword = docker exec im-learn-mysql printenv MYSQL_ROOT_PASSWORD
    if ($LASTEXITCODE -ne 0 -or !$mysqlPassword) { throw 'Cannot read local MySQL test configuration' }
    $dsn = "root:${mysqlPassword}@tcp(127.0.0.1:3306)/${database}?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai"
    $secret = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
    $topicNames = @('message-create-requested-topic','message-persisted-topic','redpacket-cache-build-topic','message-consumer-dlq-topic')
    foreach ($topic in $topicNames) {
        docker exec im-learn-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$topic-$runID" --partitions 3 --replication-factor 1
        if ($LASTEXITCODE -ne 0) { throw 'Cannot initialize acceptance Kafka topics' }
    }
    $services = @(
        @{Name='rpc'; Package='./service/user/rpc'; Config='service/user/rpc/etc/user.example.yaml'},
        @{Name='user'; Package='./service/user/api'; Config='service/user/api/etc/user-api.example.yaml'},
        @{Name='offline'; Package='./service/offline'; Config='service/offline/etc/offline.example.yaml'},
        @{Name='realtime'; Package='./service/realtime'; Config='service/realtime/etc/realtime.example.yaml'},
        @{Name='redpacket'; Package='./service/redpacket/api'; Config='service/redpacket/api/etc/redpacket-api.example.yaml'},
        @{Name='worker'; Package='./service/redpacket/worker'; Config='service/redpacket/worker/etc/redpacket-worker.example.yaml'},
        @{Name='relay'; Package='./service/outboxrelay'; Config='service/outboxrelay/etc/outbox-relay.example.yaml'},
        @{Name='gateway'; Package='./service/gateway'; Config='service/gateway/etc/gateway.example.yaml'}
    )
    foreach ($service in $services) {
        Write-Host "Build $($service.Name)"
        $exe = Join-Path $runDir "$($service.Name).exe"
        go build -o $exe $service.Package
        if ($LASTEXITCODE -ne 0) { throw "Build failed: $($service.Name)" }
        $config = Get-Content -Encoding utf8 -Raw $service.Config
        $config = [regex]::Replace($config, '(?m)^DataSource:.*$', "DataSource: $dsn")
        $config = [regex]::Replace($config, '(?m)^(\s*)AccessSecret:.*$', "`$1AccessSecret: $secret")
        if ($service.Name -eq 'user') {
            $minioUser = docker exec im-learn-minio printenv MINIO_ROOT_USER
            $minioPassword = docker exec im-learn-minio printenv MINIO_ROOT_PASSWORD
            $config = [regex]::Replace($config, '(?m)^(\s*)AccessKey:.*$', "`$1AccessKey: $minioUser")
            $config = [regex]::Replace($config, '(?m)^(\s*)SecretKey:.*$', "`$1SecretKey: $minioPassword")
        }
        $config = [regex]::Replace($config, '(?m)^(\s*)Key: user\.rpc\s*$', "`$1Key: acceptance.user.rpc.$runID")
        foreach ($topic in $topicNames) { $config = $config.Replace($topic, "$topic-$runID") }
        $config = [regex]::Replace($config, '(?m)^(\s*(?:MessageWriterGroup|PushGroup|MessageCacheGroup|CacheBuildGroup):\s*)([^\r\n]+)', "`$1`$2-$runID")
        $configPath = Join-Path $runDir "$($service.Name).yaml"
        [IO.File]::WriteAllText($configPath, $config, [Text.UTF8Encoding]::new($false))
    }
    go build -o (Join-Path $runDir 'helper.exe') ./scripts/acceptance-tools
    if ($LASTEXITCODE -ne 0) { throw 'Acceptance helper build failed' }
    $env:IM_MYSQL_DSN = $dsn
    $env:IM_ACCEPTANCE_DB = $database
    $env:IM_ACCEPTANCE_RUN_DIR = $runDir
    $env:IM_ACCEPTANCE_RUN_ID = $runID
    node scripts/project-acceptance.mjs --seed
    if ($LASTEXITCODE -ne 0) { throw 'Acceptance fixtures failed' }
    foreach ($service in $services) {
        $arguments = @('-f', (Join-Path $runDir "$($service.Name).yaml"))
        if ($service.Name -eq 'gateway') { $arguments += @('-static', (Resolve-Path ../webark-im-template/dist/demo).Path) }
        $process = Start-Process -FilePath (Join-Path $runDir "$($service.Name).exe") -ArgumentList $arguments -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runDir "$($service.Name).stdout.log") -RedirectStandardError (Join-Path $runDir "$($service.Name).stderr.log")
        $processes += $process
    }
    $processes | Select-Object Id,ProcessName | ConvertTo-Json | Set-Content -Encoding utf8 (Join-Path $runDir 'processes.json')
    node scripts/project-acceptance.mjs
    if ($LASTEXITCODE -ne 0) { throw 'Project acceptance found a failure; inspect results.json and service logs' }
    node ../webark-im-template/scripts/imlearn-ui-acceptance.mjs
    if ($LASTEXITCODE -ne 0) { throw 'Built frontend browser acceptance failed' }
    $frontend = (Resolve-Path ../webark-im-template).Path
    $vite = Start-Process -FilePath (Get-Command node).Source -ArgumentList @('node_modules/vite/bin/vite.js','--host','127.0.0.1','--port','42173','--strictPort') -WorkingDirectory $frontend -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runDir 'vite.stdout.log') -RedirectStandardError (Join-Path $runDir 'vite.stderr.log')
    $processes += $vite
    $ready = $false
    $deadline = (Get-Date).AddSeconds(20)
    while ((Get-Date) -lt $deadline) {
        try { $null = Invoke-WebRequest 'http://127.0.0.1:42173/' -UseBasicParsing -TimeoutSec 2; $ready = $true; break } catch { Start-Sleep -Milliseconds 200 }
    }
    if (!$ready) { throw 'Vite test server did not start' }
    $fixture = Get-Content (Join-Path $runDir 'fixtures.json') -Raw -Encoding utf8 | ConvertFrom-Json
    $env:IM_ACCEPTANCE_SESSION_ID = $fixture.singleID
    $env:IM_ACCEPTANCE_ALICE_ACCOUNT = $fixture.users[0].account
    $env:IM_ACCEPTANCE_BOB_ACCOUNT = $fixture.users[1].account
    $env:IM_ACCEPTANCE_FRONTEND_URL = 'http://127.0.0.1:42173'
    $handoff = node ../webark-im-template/scripts/imlearn-live-handoff.mjs
    if ($LASTEXITCODE -ne 0) { throw 'Offline/realtime browser handoff failed' }
    [IO.File]::WriteAllText((Join-Path $runDir 'browser-handoff.json'), ($handoff -join "`n"), [Text.UTF8Encoding]::new($false))
    Write-Host $handoff
    Write-Host "Acceptance passed. Results: $runDir/results.json"
}
finally {
    # Restore fault-injected containers even when an assertion aborts the run.
    docker start im-learn-redis im-learn-kafka | Out-Null
    if (!$KeepRunning) {
        foreach ($process in $processes) {
            if (!$process.HasExited) { Stop-Process -Id $process.Id -ErrorAction SilentlyContinue }
        }
    }
    Remove-Item Env:IM_MYSQL_DSN -ErrorAction SilentlyContinue
}

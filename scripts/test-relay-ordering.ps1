$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
$relayTestDatabase = 'im_relay_test_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
$relayTestDDL = (Get-Content sql/outbox_event.sql -Raw -Encoding UTF8).Replace('USE `infiniteChat`;', "USE $relayTestDatabase;")
$relayTestDDL = [regex]::Replace($relayTestDDL, ',\s*INDEX `idx_relay_predecessor`[^\r\n]+', '')
"CREATE DATABASE $relayTestDatabase; $relayTestDDL" | docker exec -i im-learn-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot'
if ($LASTEXITCODE -ne 0) { throw 'Isolated schema setup failed' }
$relayTestPassword = docker exec im-learn-mysql printenv MYSQL_ROOT_PASSWORD
$env:IM_RELAY_TEST_DSN = "root:${relayTestPassword}@tcp(127.0.0.1:3306)/${relayTestDatabase}?parseTime=true&loc=Asia%2FShanghai"
try {
    go test -tags integration ./service/outboxrelay/internal/relay -run TestMySQLPredecessorOrderingAndRecovery -count=1 -v
    if ($LASTEXITCODE -ne 0) { throw 'Relay database ordering test failed' }
} finally { Remove-Item Env:IM_RELAY_TEST_DSN -ErrorAction SilentlyContinue }

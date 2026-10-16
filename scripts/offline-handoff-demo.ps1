param(
    [string]$MessagePrefix = 'offline-handoff',
    [string]$Password = 'Test123!'
)

$ErrorActionPreference = 'Stop'
$apiBase = 'http://127.0.0.1:8104'
$offlineBase = 'http://127.0.0.1:8105'
$wsUri = [Uri]'ws://127.0.0.1:9101/ws/chat'
$sessionId = '2100779545543053312'
$bobId = '2100779274846867456'

function Login-TestUser([string]$account) {
    $body = @{ account = $account; password = $Password } | ConvertTo-Json
    $response = Invoke-RestMethod -Uri "$apiBase/api/user/loginPassword" -Method Post -ContentType 'application/json' -Body $body
    if ($response.code -ne 200 -or -not $response.data.token) { throw "Login failed: $account" }
    return $response.data
}

function Connect-Chat([string]$token) {
    $socket = [System.Net.WebSockets.ClientWebSocket]::new()
    $socket.Options.SetRequestHeader('Authorization', "Bearer $token")
    $null = $socket.ConnectAsync($wsUri, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
    $ready = Receive-Chat $socket
    if ($ready.eventType -ne 'connection.ready') {
        $socket.Dispose()
        throw "Expected connection.ready, got $($ready.eventType)"
    }
    return $socket
}

function Receive-Chat($socket, [int]$timeoutSeconds = 30) {
    $buffer = New-Object byte[] 8192
    $segment = [ArraySegment[byte]]::new($buffer)
    $timeout = [Threading.CancellationTokenSource]::new([TimeSpan]::FromSeconds($timeoutSeconds))
    try {
        $result = $socket.ReceiveAsync($segment, $timeout.Token).GetAwaiter().GetResult()
        if ($result.MessageType -ne [System.Net.WebSockets.WebSocketMessageType]::Text -or -not $result.EndOfMessage) {
            throw 'Unexpected WebSocket frame'
        }
        return ([Text.Encoding]::UTF8.GetString($buffer, 0, $result.Count) | ConvertFrom-Json)
    }
    finally { $timeout.Dispose() }
}

function Sync-Chat([string]$token, [string]$afterSeq) {
    $body = @{
        conversations = @(@{ sessionId = $sessionId; afterSeq = $afterSeq })
        limit = 200
    } | ConvertTo-Json -Depth 5 -Compress
    $response = Invoke-RestMethod -Uri "$offlineBase/api/offline/sync" -Method Post `
        -Headers @{ Authorization = "Bearer $token" } -ContentType 'application/json' -Body $body
    if ($response.code -ne 200 -or $response.data.conversations.Count -ne 1) { throw 'Offline sync failed' }
    return $response.data.conversations[0]
}

function Send-Chat($socket, [string]$content) {
    $clientMessageId = 'handoff-' + [Guid]::NewGuid().ToString('N')
    $payload = @{
        eventType = 'message.send'
        clientMessageId = $clientMessageId
        sessionId = $sessionId
        sessionType = 0
        receiverId = $bobId
        messageType = 0
        body = @{ content = $content }
    } | ConvertTo-Json -Depth 5 -Compress
    $bytes = [Text.Encoding]::UTF8.GetBytes($payload)
    $null = $socket.SendAsync([ArraySegment[byte]]::new($bytes),
        [System.Net.WebSockets.WebSocketMessageType]::Text, $true,
        [Threading.CancellationToken]::None).GetAwaiter().GetResult()
    $ack = Receive-Chat $socket
    if ($ack.eventType -ne 'message.ack' -or $ack.status -ne 'stored' -or
        $ack.clientMessageId -ne $clientMessageId) {
        throw "Unexpected send response: $($ack.eventType) $($ack.status)"
    }
    return $ack
}

$alice = Login-TestUser '13800000001'
$bob = Login-TestUser '13800000002'
$bobSocket = Connect-Chat $bob.token
$aliceSocket = Connect-Chat $alice.token

try {
    $baseline = Sync-Chat $bob.token '0'
    $firstAck = Send-Chat $aliceSocket "$MessagePrefix-live"
    $push = Receive-Chat $bobSocket
    if ($push.eventType -ne 'message.new' -or $push.message.messageId -ne $firstAck.messageId) {
        throw 'Realtime push did not match the stored message'
    }
    $overlap = Sync-Chat $bob.token $baseline.nextSeq
    if (-not ($overlap.messages | Where-Object { $_.messageId -eq $push.message.messageId })) {
        throw 'The live message was not returned by offline sync from the old cursor'
    }

    $bobSocket.Dispose()
    $secondAck = Send-Chat $aliceSocket "$MessagePrefix-disconnected"
    $catchUp = Sync-Chat $bob.token $overlap.nextSeq
    if (-not ($catchUp.messages | Where-Object { $_.messageId -eq $secondAck.messageId })) {
        throw 'The message sent while disconnected was not recovered by sync'
    }

    [pscustomobject]@{
        ReadyConfirmed = $true
        BaselineSeq = $baseline.nextSeq
        LiveMessageID = $push.message.messageId
        LiveSeq = $push.message.seq
        LiveAlsoInSync = $true
        DisconnectedMessageID = $secondAck.messageId
        RecoveredSeq = $catchUp.nextSeq
    }
}
finally {
    $aliceSocket.Dispose()
    $bobSocket.Dispose()
}

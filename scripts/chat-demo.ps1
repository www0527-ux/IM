param(
    [string]$Message = "Hello from IM-learn!"
)

$ErrorActionPreference = "Stop"
$apiBase = "http://127.0.0.1:8104"
$wsUri = [Uri]"ws://127.0.0.1:9101/ws/chat"
$sessionId = "2100779545543053312"
$aliceAccount = "13800000001"
$aliceId = "2100779274364522496"
$bobAccount = "13800000002"
$bobId = "2100779274846867456"
$password = "Test123!"

function Login-TestUser([string]$Account) {
    $body = @{
        account = $Account
        password = $password
    } | ConvertTo-Json

    return (Invoke-RestMethod `
        -Uri "$apiBase/api/user/loginPassword" `
        -Method Post `
        -ContentType "application/json" `
        -Body $body).data
}

function Connect-Chat([string]$Token) {
    $socket = [System.Net.WebSockets.ClientWebSocket]::new()
    $socket.Options.SetRequestHeader("Authorization", "Bearer $Token")
    $null = $socket.ConnectAsync(
        $wsUri,
        [Threading.CancellationToken]::None
    ).GetAwaiter().GetResult()
    return $socket
}

function Send-Chat($Socket, [string]$Json) {
    $bytes = [Text.Encoding]::UTF8.GetBytes($Json)
    $segment = [ArraySegment[byte]]::new($bytes)
    $null = $Socket.SendAsync(
        $segment,
        [System.Net.WebSockets.WebSocketMessageType]::Text,
        $true,
        [Threading.CancellationToken]::None
    ).GetAwaiter().GetResult()
}

function Receive-Chat($Socket, [int]$TimeoutSeconds = 10) {
    $buffer = New-Object byte[] 8192
    $segment = [ArraySegment[byte]]::new($buffer)
    $timeout = [Threading.CancellationTokenSource]::new(
        [TimeSpan]::FromSeconds($TimeoutSeconds)
    )
    try {
        $result = $Socket.ReceiveAsync(
            $segment,
            $timeout.Token
        ).GetAwaiter().GetResult()
        return [Text.Encoding]::UTF8.GetString(
            $buffer,
            0,
            $result.Count
        )
    }
    finally {
        $timeout.Dispose()
    }
}

$alice = Login-TestUser $aliceAccount
$bob = Login-TestUser $bobAccount
$aliceSocket = Connect-Chat $alice.token
$bobSocket = Connect-Chat $bob.token

try {
    $clientMessageId = "demo-" + [Guid]::NewGuid().ToString("N")
    $payload = [ordered]@{
        eventType = "message.send"
        clientMessageId = $clientMessageId
        sessionId = $sessionId
        sessionType = 0
        receiverId = $bobId
        messageType = 0
        body = @{
            content = $Message
        }
    } | ConvertTo-Json -Depth 6 -Compress

    Write-Host "Alice($aliceId) -> Bob($bobId)"
    Write-Host "SEND    $payload"
    Send-Chat $aliceSocket $payload
    Write-Host "ALICE-1 $(Receive-Chat $aliceSocket)"
    Write-Host "ALICE-2 $(Receive-Chat $aliceSocket)"
    Write-Host "BOB     $(Receive-Chat $bobSocket)"
}
finally {
    $aliceSocket.Dispose()
    $bobSocket.Dispose()
}

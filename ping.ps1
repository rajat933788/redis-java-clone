param(
    [int]$Port = 6379,
    [string]$HostName = "127.0.0.1"
)

try {
    $client = New-Object System.Net.Sockets.TcpClient($HostName, $Port)
    $stream = $client.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    $writer.AutoFlush = $true
    $reader = New-Object System.IO.StreamReader($stream)

    # 1. PING
    $pingcommand = "*1`r`n`$4`r`nPING`r`n"
    $writer.Write($pingcommand)
    $response = $reader.ReadLine()
    Write-Host "PING Response: $response"

    # 2. SET foo bar
    $setcommand = "*3`r`n`$3`r`nSET`r`n`$3`r`nfoo`r`n`$3`r`nbar`r`n"
    $writer.Write($setcommand)
    $response = $reader.ReadLine()
    Write-Host "SET Response: $response"

    # 3. GET foo
    $getcommand = "*2`r`n`$3`r`nGET`r`n`$3`r`nfoo`r`n"
    $writer.Write($getcommand)
    $response = $reader.ReadLine()
    $val = $reader.ReadLine()
    Write-Host "GET Response: $response $val"

    $reader.Close()
    $writer.Close()
    $client.Close()
} catch {
    Write-Error "Failed to connect or communicate with Redis at ${HostName}:${Port}: $_"
}
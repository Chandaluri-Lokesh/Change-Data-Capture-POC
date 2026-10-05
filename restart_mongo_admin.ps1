# Run this script as Administrator to reconfigure MongoDB with replica set mode
# Right-click PowerShell -> "Run as administrator", then: .\restart_mongo_admin.ps1

$cfg = "C:\Program Files\MongoDB\Server\8.2\bin\mongod.cfg"

$newConfig = @"
# mongod.conf
storage:
  dbPath: C:\Program Files\MongoDB\Server\8.2\data

systemLog:
  destination: file
  logAppend: true
  path: C:\Program Files\MongoDB\Server\8.2\log\mongod.log

net:
  port: 27018
  bindIpAll: true

replication:
  replSetName: "rs0"
"@

Write-Host "Writing new mongod.cfg..."
Set-Content -Path $cfg -Value $newConfig -Encoding UTF8

Write-Host "Restarting MongoDB service..."
net stop MongoDB
Start-Sleep -Seconds 2
net start MongoDB
Start-Sleep -Seconds 3

Write-Host "Done. MongoDB is now running on port 27018 with replica set rs0 on all interfaces."

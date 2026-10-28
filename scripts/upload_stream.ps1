# Uploads the Kafka-style message files into the stream/ folder (file source for the multiplexed bronze table).
# Control how many to send, so you can test arrival in steps.
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\scripts\upload_stream.ps1 -Catalog freight_dev -Dir "D:\...\data\stream" -Files 2
#   powershell -ExecutionPolicy Bypass -File .\scripts\upload_stream.ps1 -Catalog freight_dev -Dir "D:\...\data\stream" -Skip 2     # the rest
param([string]$Catalog, [string]$Dir, [int]$Files = 0, [int]$Skip = 0)

$all = Get-ChildItem "$Dir\kafka_batch_*.json" | Sort-Object Name | Select-Object -Skip $Skip
if ($Files -gt 0) { $all = $all | Select-Object -First $Files }
foreach ($f in $all) {
    databricks fs cp $f.FullName "dbfs:/Volumes/$Catalog/landing/raw/stream/$($f.Name)" --overwrite
}
Write-Host "Uploaded $($all.Count) file(s)"

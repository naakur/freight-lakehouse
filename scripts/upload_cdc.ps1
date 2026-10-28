# Sends the carrier change feed in two waves (A = files 1+2, B = file 3: late + broken records).
# Usage:  powershell -ExecutionPolicy Bypass -File .\scripts\upload_cdc.ps1 -Catalog freight_dev -Wave A
param([string]$Catalog, [ValidateSet("A","B")][string]$Wave, [string]$Dir = ".\sample_cdc")
$files = if ($Wave -eq "A") { "carriers_cdc_0001.csv","carriers_cdc_0002.csv" } else { "carriers_cdc_0003.csv" }
foreach ($f in $files) {
    databricks fs cp "$Dir\$f" "dbfs:/Volumes/$Catalog/landing/raw/inbox/carriers_cdc/$f" --overwrite
}
Write-Host "Wave $Wave uploaded"

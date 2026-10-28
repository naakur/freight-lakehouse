# One-time: store the Kafka credentials in a Databricks secret scope (nothing goes into the repo).
# Usage:  powershell -ExecutionPolicy Bypass -File .\scripts\setup_secrets.ps1
param([string]$Scope = "freight-kafka")

databricks secrets create-scope $Scope
Write-Host "Paste each value when prompted. If your CLI does not prompt, use:"
Write-Host "  databricks secrets put-secret $Scope <key> --string-value <value>   (then clear your shell history)"
foreach ($key in "kafka-bootstrap", "kafka-api-key", "kafka-api-secret") {
    Write-Host "Secret: $key"
    databricks secrets put-secret $Scope $key
}
databricks secrets list-secrets $Scope

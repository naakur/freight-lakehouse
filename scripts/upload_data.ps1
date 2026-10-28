# Windows PowerShell version of upload_data.sh
# Usage:
#   .\scripts\upload_data.ps1 -Catalog freight_dev -Dir "D:\path\to\csvs"
#   .\scripts\upload_data.ps1 -Catalog freight_dev -Dir .\sample_inbox_bad -Bad
param([string]$Catalog, [string]$Dir, [switch]$Bad)
$base = "dbfs:/Volumes/$Catalog/landing/raw/inbox"

if ($Bad) {
    databricks fs cp "$Dir\invoices_wrong_header.csv" "$base/invoices/invoices_wrong_header.csv" --overwrite
    databricks fs cp "$Dir\invoices_notes.txt"        "$base/invoices/invoices_notes.txt"        --overwrite
    databricks fs cp "$Dir\invoices_bad_rows.csv"     "$base/invoices/invoices_bad_rows.csv"     --overwrite
    databricks fs cp "$Dir\shipments_empty.csv"       "$base/shipments/shipments_empty.csv"      --overwrite
    databricks fs cp "$Dir\gl_codes_header_only.csv"  "$base/gl_codes/gl_codes_header_only.csv"  --overwrite
} else {
    foreach ($src in "carriers","rate_cards","accessorial_codes","gl_codes","shipments","invoices") {
        databricks fs cp "$Dir\$src.csv" "$base/$src/$src.csv" --overwrite
    }
}
Write-Host "Uploaded to $base"

param(
  [string]$BaseUrl = "http://localhost:7079",
  [string]$Template = "RFID-RAW-P1.template.zpl",
  [string]$ProfileKey = "P1:RAW",
  [string]$LotNumber = "PT000086",
  [string]$BoxNumber = "52",
  [string]$Rfid = "",
  [string]$Pounds = "_",
  [string]$MaterialType = "POLYPROPYLENE",
  [string]$Color = "ULTRAMARINEBLUE",
  [string]$Po = "PO12345",
  [string]$ProductDescription = "Template Lab Product",
  [string]$Tolling = "",
  [string]$ProfileOverridesJson = "",
  [string]$ProfileOverridesPath = "",
  [string]$PrinterIp = "",
  [int]$Port = 9100,
  [switch]$Send
)

$ErrorActionPreference = "Stop"

$body = @{
  template = $Template
  profileKey = $ProfileKey
  lotNumber = $LotNumber
  boxNumber = $BoxNumber
  rfid = $Rfid
  pounds = $Pounds
  materialType = $MaterialType
  color = $Color
  po = $Po
  productDescription = $ProductDescription
  tolling = $Tolling
}

if ($ProfileOverridesPath) {
  if (-not (Test-Path -LiteralPath $ProfileOverridesPath)) {
    throw "ProfileOverridesPath not found: $ProfileOverridesPath"
  }
  $ProfileOverridesJson = Get-Content -Raw -LiteralPath $ProfileOverridesPath
}

if ($ProfileOverridesJson) {
  $parsedOverrides = $ProfileOverridesJson | ConvertFrom-Json -Depth 20
  if ($parsedOverrides.overrides) {
    $body.profileOverrides = $parsedOverrides.overrides
  } else {
    $body.profileOverrides = $parsedOverrides
  }
}

$preview = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/print/template-preview" -ContentType "application/json" -Body ($body | ConvertTo-Json -Depth 20)

$outDir = Join-Path (Get-Location) "rendered"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$safeTemplate = [IO.Path]::GetFileNameWithoutExtension($Template) -replace '[^A-Za-z0-9_.-]', '_'
$outPath = Join-Path $outDir "$safeTemplate.rendered.zpl"
Set-Content -Path $outPath -Value $preview.renderedZpl -Encoding ASCII

Write-Host "Rendered template: $Template"
Write-Host "Profile: $($preview.profileKey)"
Write-Host "Payload bytes: $($preview.metadata.payloadBytes)"
Write-Host "QR command: $($preview.metadata.qr.command)"
Write-Host "QR payload: $($preview.metadata.qr.payload)"
Write-Host "Preview mode: $($preview.metadata.previewMode)"
Write-Host "Saved profile path: $($preview.profileConfigPath)"
Write-Host "Wrote: $outPath"

if ($Send) {
  if (-not $PrinterIp) {
    throw "PrinterIp is required when using -Send."
  }

  $sendBody = $body.Clone()
  $sendBody.printerIp = $PrinterIp
  $sendBody.port = $Port
  $sendBody.confirmTestPrint = $true
  $sendResult = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/print/template-test-send" -ContentType "application/json" -Body ($sendBody | ConvertTo-Json -Depth 20)
  Write-Host "Proof print sent: $($sendResult.message)"
}

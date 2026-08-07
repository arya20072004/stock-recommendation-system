param (
    [switch]$DryRun,
    [switch]$SkipCollection,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# Locate the directory of this script, then move to project root
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectRoot = Resolve-Path "$ScriptDir\.."

Set-Location -Path $ProjectRoot

# Activate Virtual Environment (assumes .venv exists)
$VenvPath = "$ProjectRoot\.venv\Scripts\Activate.ps1"
if (Test-Path $VenvPath) {
    . $VenvPath
} else {
    Write-Warning "Virtual environment not found at $VenvPath. Using global python."
}

# Construct arguments
$Args = @()
if ($DryRun) { $Args += "--dry-run" }
if ($SkipCollection) { $Args += "--skip-collection" }
if ($Force) { $Args += "--force" }

Write-Output "Starting production pipeline from $ProjectRoot"
python -m src.pipeline.daily @Args

$ExitCode = $LASTEXITCODE

if ($ExitCode -eq 0) {
    Write-Output "Pipeline finished SUCCESSFULLY."
} elseif ($ExitCode -eq 2) {
    Write-Output "Pipeline finished in DEGRADED state."
} elseif ($ExitCode -eq 3) {
    Write-Output "Pipeline execution was BLOCKED."
} else {
    Write-Output "Pipeline FAILED with exit code $ExitCode."
}

exit $ExitCode

<#
Windows 10 PowerShell 启动脚本（对应 fpbrowser2api_service.sh）

用法:
  powershell -ExecutionPolicy Bypass -File .\fpbrowser2api_service.ps1 start|stop|restart|status

可选环境变量（和 .sh 保持一致的命名）:
  PYTHON_BIN            指定 python 可执行文件路径
  PID_FILE              pid 文件路径
  LOG_FILE              服务输出日志（stdout）
  LOG_ERR_FILE          服务错误日志（stderr）。不设置则默认 LOG_FILE + ".err"
  DEBUG_LOG_FILE         调试日志（logs.txt）
  APP_LOG_FILE            应用日志（app.log）
  LOGS_DIR              轮转目录
#>

[CmdletBinding()]
param(
  [Parameter(Position = 0)]
  [ValidateSet("start", "stop", "restart", "status")]
  [string]$Command = ""
)

$ErrorActionPreference = "Stop"

function Get-AppDir {
  if ($PSScriptRoot) { return $PSScriptRoot }
  return (Split-Path -Parent $MyInvocation.MyCommand.Path)
}

$APP_DIR = Get-AppDir

function Get-EnvOrDefault([string]$name, [string]$defaultValue) {
  $v = [Environment]::GetEnvironmentVariable($name)
  if ([string]::IsNullOrWhiteSpace($v)) { return $defaultValue }
  return $v
}

$PID_FILE       = Get-EnvOrDefault "PID_FILE"       (Join-Path $APP_DIR "fpbrowser2api.pid")
$LOG_FILE       = Get-EnvOrDefault "LOG_FILE"       (Join-Path $APP_DIR "fpbrowser2api.out")
$LOG_ERR_FILE   = Get-EnvOrDefault "LOG_ERR_FILE"   ($LOG_FILE + ".err")
$LOGS_DIR       = Get-EnvOrDefault "LOGS_DIR"       (Join-Path $APP_DIR "logs")
$DEBUG_LOG_FILE = Get-EnvOrDefault "DEBUG_LOG_FILE" (Join-Path $APP_DIR "logs.txt")
$APP_LOG_FILE   = Get-EnvOrDefault "APP_LOG_FILE"   (Join-Path $APP_DIR "app.log")

function Resolve-PythonBin {
  $py = [Environment]::GetEnvironmentVariable("PYTHON_BIN")
  if (-not [string]::IsNullOrWhiteSpace($py)) { return $py }

  $candidates = @(
    (Join-Path $APP_DIR ".venv\Scripts\python.exe"),
    (Join-Path $APP_DIR "venv\Scripts\python.exe"),
    (Join-Path $APP_DIR ".venv\bin\python"),
    (Join-Path $APP_DIR "venv\bin\python")
  )

  foreach ($c in $candidates) {
    if (Test-Path $c) { return $c }
  }

  # 最后兜底：PATH 里的 python
  return "python"
}

$PYTHON_BIN = Resolve-PythonBin

function Get-PidFromFile {
  if (-not (Test-Path $PID_FILE)) { return $null }
  $raw = (Get-Content -Path $PID_FILE -ErrorAction SilentlyContinue | Select-Object -First 1)
  if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
  $procIdValue = 0
  if (-not [int]::TryParse($raw.Trim(), [ref]$procIdValue)) { return $null }
  return $procIdValue
}

function Test-IsRunning {
  $procId = Get-PidFromFile
  if (-not $procId) { return $false }
  try {
    $p = Get-Process -Id $procId -ErrorAction Stop
    return ($null -ne $p)
  } catch {
    return $false
  }
}

function Ensure-FileExists([string]$path) {
  $dir = Split-Path -Parent $path
  if (-not [string]::IsNullOrWhiteSpace($dir)) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
  }
  if (-not (Test-Path $path)) {
    New-Item -ItemType File -Path $path -Force | Out-Null
  }
}

function Truncate-File([string]$path) {
  Ensure-FileExists $path
  Set-Content -Path $path -Value "" -Encoding UTF8
}

function Rotate-And-Truncate([string]$src, [string]$prefix) {
  New-Item -ItemType Directory -Path $LOGS_DIR -Force | Out-Null
  Ensure-FileExists $src

  $item = Get-Item -Path $src -ErrorAction SilentlyContinue
  if ($item -and $item.Length -gt 0) {
    $ts = Get-Date -Format "yyyyMMdd_HHmmss"
    $rand = Get-Random -Minimum 10000 -Maximum 99999
    $dest = Join-Path $LOGS_DIR ("{0}_{1}_{2}.txt" -f $prefix, $ts, $rand)
    Move-Item -Path $src -Destination $dest -Force
    Write-Host ("已备份旧日志: {0} -> {1}" -f $src, $dest)
  }

  Truncate-File $src
}

function Start-ServiceProcess {
  if (Test-IsRunning) {
    $procId = Get-PidFromFile
    Write-Host ("fpbrowser2api 已在运行 (pid={0})" -f $procId)
    return
  }

  Set-Location -Path $APP_DIR

  # 启动前：备份并清空调试日志（logs.txt）
  Rotate-And-Truncate $DEBUG_LOG_FILE "logs"

  # 启动前：备份并清空 app.log（当 log_to_file=true 才会写入）
  if (Test-Path $APP_LOG_FILE) {
    Rotate-And-Truncate $APP_LOG_FILE "app"
  }

  # 启动前：清空服务输出日志
  Truncate-File $LOG_FILE
  Truncate-File $LOG_ERR_FILE

  $mainPy = Join-Path $APP_DIR "main.py"
  if (-not (Test-Path $mainPy)) {
    throw ("未找到 main.py: {0}" -f $mainPy)
  }

  # 后台运行（启动方式：python main.py）
  $proc = Start-Process `
    -FilePath $PYTHON_BIN `
    -ArgumentList @("`"$mainPy`"") `
    -WorkingDirectory $APP_DIR `
    -WindowStyle Hidden `
    -RedirectStandardOutput $LOG_FILE `
    -RedirectStandardError $LOG_ERR_FILE `
    -PassThru

  Set-Content -Path $PID_FILE -Value $proc.Id -Encoding ASCII

  Start-Sleep -Seconds 1
  if (Test-IsRunning) {
    Write-Host ("fpbrowser2api 启动成功 (pid={0}), log={1}, err={2}" -f (Get-PidFromFile), $LOG_FILE, $LOG_ERR_FILE)
    return
  }

  throw ("fpbrowser2api 启动失败，请查看日志: {0} / {1}" -f $LOG_FILE, $LOG_ERR_FILE)
}

function Stop-ServiceProcess {
  $procId = Get-PidFromFile
  if (-not $procId) {
    if (Test-Path $PID_FILE) { Remove-Item -Path $PID_FILE -Force -ErrorAction SilentlyContinue }
    Write-Host ("fpbrowser2api 未运行（找不到 pid 或 pid 文件为空: {0}）" -f $PID_FILE)
    return
  }

  try {
    $p = Get-Process -Id $procId -ErrorAction Stop
  } catch {
    Remove-Item -Path $PID_FILE -Force -ErrorAction SilentlyContinue
    Write-Host "fpbrowser2api 进程不存在，已清理 pid 文件"
    return
  }

  Write-Host ("正在停止 fpbrowser2api (pid={0})..." -f $procId)
  try { Stop-Process -Id $procId -ErrorAction SilentlyContinue } catch {}

  $deadline = (Get-Date).AddSeconds(30)
  while ((Get-Date) -lt $deadline) {
    try {
      Get-Process -Id $procId -ErrorAction Stop | Out-Null
      Start-Sleep -Seconds 1
    } catch {
      break
    }
  }

  $stillRunning = $false
  try {
    Get-Process -Id $procId -ErrorAction Stop | Out-Null
    $stillRunning = $true
  } catch {
    $stillRunning = $false
  }

  if ($stillRunning) {
    Write-Host ("优雅停止超时，强制杀进程 (pid={0})" -f $procId)
    try { Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue } catch {}
  }

  Remove-Item -Path $PID_FILE -Force -ErrorAction SilentlyContinue
  Write-Host "fpbrowser2api 已停止"
}

function Show-Status {
  if (Test-IsRunning) {
    Write-Host ("fpbrowser2api 运行中 (pid={0})" -f (Get-PidFromFile))
  } else {
    Write-Host "fpbrowser2api 未运行"
  }
}

function Show-Usage {
  @"
用法:
  powershell -ExecutionPolicy Bypass -File .\fpbrowser2api_service.ps1 start|stop|restart|status

可选环境变量:
  PYTHON_BIN=...\python.exe
  PID_FILE=...\fpbrowser2api.pid
  LOG_FILE=...\fpbrowser2api.out
  LOG_ERR_FILE=...\fpbrowser2api.err
  DEBUG_LOG_FILE=...\logs.txt
  APP_LOG_FILE=...\app.log
  LOGS_DIR=...\logs
"@ | Write-Host
}

switch ($Command) {
  "start"   { Start-ServiceProcess }
  "stop"    { Stop-ServiceProcess }
  "restart" { Stop-ServiceProcess; Start-ServiceProcess }
  "status"  { Show-Status }
  default   { Show-Usage; exit 1 }
}


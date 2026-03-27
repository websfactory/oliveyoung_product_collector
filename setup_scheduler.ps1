# 올리브영 수집기 Windows 작업 스케줄러 등록 스크립트
# 실행 방법: 관리자 권한 PowerShell에서 실행
#   .\setup_scheduler.ps1

# 관리자 권한 확인
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Host "[ERROR] 관리자 권한으로 PowerShell을 실행해주세요." -ForegroundColor Red
    exit 1
}

$pythonPath = "C:\Users\owner\AppData\Local\Programs\Python\Python311\python.exe"
$workDir = "C:\Users\owner\Documents\oliveyoung_collector_clean\oliveyoung_collector_clean"

# Python 경로 확인
if (-NOT (Test-Path $pythonPath)) {
    Write-Host "[ERROR] Python을 찾을 수 없습니다: $pythonPath" -ForegroundColor Red
    exit 1
}

# 작업 디렉토리 확인
if (-NOT (Test-Path $workDir)) {
    Write-Host "[ERROR] 작업 디렉토리를 찾을 수 없습니다: $workDir" -ForegroundColor Red
    exit 1
}

Write-Host "=== 올리브영 수집기 스케줄러 등록 ===" -ForegroundColor Cyan
Write-Host "Python: $pythonPath"
Write-Host "작업 디렉토리: $workDir"
Write-Host ""

# 1. main.py - 월~토 새벽 4시
$taskName1 = "OliveYoung_Main"
Unregister-ScheduledTask -TaskName $taskName1 -Confirm:$false -ErrorAction SilentlyContinue

$action1 = New-ScheduledTaskAction `
    -Execute $pythonPath `
    -Argument "main.py --local" `
    -WorkingDirectory $workDir

$trigger1 = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday,Saturday -At 4:00AM

Register-ScheduledTask -TaskName $taskName1 -Action $action1 -Trigger $trigger1 `
    -Description "올리브영 카테고리 수집 (월~토 04:00)"

Write-Host "[OK] $taskName1 등록 완료 (월~토 04:00)" -ForegroundColor Green

# 2. retry_missing_products.py - 일요일 새벽 4시
$taskName2 = "OliveYoung_Retry"
Unregister-ScheduledTask -TaskName $taskName2 -Confirm:$false -ErrorAction SilentlyContinue

$action2 = New-ScheduledTaskAction `
    -Execute $pythonPath `
    -Argument "retry_missing_products.py" `
    -WorkingDirectory $workDir

$trigger2 = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 4:00AM

Register-ScheduledTask -TaskName $taskName2 -Action $action2 -Trigger $trigger2 `
    -Description "올리브영 누락 제품 재수집 (일 04:00)"

Write-Host "[OK] $taskName2 등록 완료 (일 04:00)" -ForegroundColor Green

# 등록 결과 확인
Write-Host ""
Write-Host "=== 등록된 작업 목록 ===" -ForegroundColor Cyan
Get-ScheduledTask -TaskName "OliveYoung_*" | Format-Table TaskName, State, Description -AutoSize

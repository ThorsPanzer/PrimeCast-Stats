@echo off
cd /d "%~dp0"
where python >nul 2>&1
if %errorlevel%==0 (
  python app.py --open-browser
) else (
  py app.py --open-browser
)
pause

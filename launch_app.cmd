@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
set "PYTHON_EXE="

where py >nul 2>nul
if %ERRORLEVEL% EQU 0 (
  py -3 "%SCRIPT_DIR%_launch_app_impl.py" %*
  exit /b %ERRORLEVEL%
)

where python >nul 2>nul
if %ERRORLEVEL% EQU 0 (
  python "%SCRIPT_DIR%_launch_app_impl.py" %*
  exit /b %ERRORLEVEL%
)

if defined LOCALAPPDATA (
  for /d %%D in ("%LOCALAPPDATA%\Programs\Python\Python*") do (
    if exist "%%~fD\python.exe" set "PYTHON_EXE=%%~fD\python.exe"
  )
)

if not defined PYTHON_EXE if defined USERPROFILE (
  if exist "%USERPROFILE%\miniconda3\python.exe" set "PYTHON_EXE=%USERPROFILE%\miniconda3\python.exe"
)
if not defined PYTHON_EXE if defined USERPROFILE (
  if exist "%USERPROFILE%\anaconda3\python.exe" set "PYTHON_EXE=%USERPROFILE%\anaconda3\python.exe"
)
if not defined PYTHON_EXE if defined USERPROFILE (
  if exist "%USERPROFILE%\mambaforge\python.exe" set "PYTHON_EXE=%USERPROFILE%\mambaforge\python.exe"
)
if not defined PYTHON_EXE if defined USERPROFILE (
  if exist "%USERPROFILE%\miniforge3\python.exe" set "PYTHON_EXE=%USERPROFILE%\miniforge3\python.exe"
)

if defined PYTHON_EXE (
  "%PYTHON_EXE%" "%SCRIPT_DIR%_launch_app_impl.py" %*
  exit /b %ERRORLEVEL%
)

echo Launcher error: Python is not available in PATH. 1>&2
exit /b 1

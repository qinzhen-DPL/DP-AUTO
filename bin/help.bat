@echo off

cls
echo **********************************************
echo DataPipeline Test Automation Framework
echo helper program
echo **********************************************
echo.


:CHANGE_CWD
echo Changing to current work directory...
cd %~dp0..


:HELP
echo Doing regression...
py -3 runner.py -h
pause
goto :EOF
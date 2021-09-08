@echo off

cls
echo **********************************************
echo DataPipeline Test Automation Framework
echo installer program
echo **********************************************
echo.


:CHANGE_CWD
echo Changing to current work directory...
cd %~dp0..
echo.


:CHECK_PYTHON
echo Installing pip dependence...
call py -3 -m pip install -r requirements.txt || goto ERROR
echo.


:YES
echo Trunk ready to go!
pause
goto :EOF


:ERROR
echo.
echo Install pip dependence met problem. Please check the message above.
echo Without dependence, Trunk won't work!!!
pause
goto :EOF
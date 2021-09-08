@echo off

cls
echo **********************************************
echo DataPipeline Test Automation Framework
echo doctor program
echo **********************************************
echo.


:CHECK_PYTHON
echo Checking Python3...
call py -3 --version || goto NO_PYTHON3
echo Python3 available
echo.

:YES
echo Ready to go!
pause
goto :EOF


:NO_PYTHON3
echo.
echo Without Python3, DP won't work! Please install Python3!
pause
goto :EOF

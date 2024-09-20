@echo off
:loop
echo Starting Python script...
python main.py
echo Python script finished, restarting in 5 seconds...
timeout /t 5
goto loop

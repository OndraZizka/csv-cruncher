@echo off
setlocal

set SCRIPT_DIR=%~dp0

for %%F in ("%SCRIPT_DIR%csv-cruncher-*-fatjar.jar") do (
    set CRUNCHER_JAR=%%F
)

java -Djava.util.logging.config.file="%SCRIPT_DIR%jul.properties" -jar "%CRUNCHER_JAR%" %*

REM Cleanup the hsqldb folder (if it exists).
rmdir /s /q "hsqldb" >nul 2>&1

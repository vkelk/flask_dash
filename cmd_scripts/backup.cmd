@echo off
for /f "tokens=1-4 delims=/ " %%i in ("%date%") do (
    set dow=%%i
    set month=%%j
    set day=%%k
    set year=%%l
)
set datestr=%month%_%day%_%year%
echo datestr is %datestr%
SET PATH=%PATH%;"c:\Program Files\PostgreSQL\10\bin"
echo path is %PATH%

set BACKUP_FILE=tweets_%datestr%.dump
set BACKUP_GLOBALS=globals_%datestr%.sql
set BACKUP_SCHEMA=schema_%datestr%.sql
echo backup file name is %BACKUP_FILE%
SET PGPASSWORD=<PassWord>
REM pg_dump -U postgres -F c -b -v -f %BACKUP_FILE% <DATABASENAME>
echo on
pg_dumpall -g -U postgres > %BACKUP_GLOBALS%
pg_dump -Fp -s -v -f %BACKUP_SCHEMA% -U postgres tweets
pg_dump -Fc -v -f %BACKUP_FILE% -U postgres tweets

@echo off
REM === Set your username here ===
if "%PSQL_USER%"=="" set /p PSQL_USER=Enter PostgreSQL username: 

echo Dropping and creating database...
psql -U %PSQL_USER% -d postgres -f scripts/drop_create.sql

echo Setting up schema...
echo Setting up tables in schema...
psql -U %PSQL_USER% -d datawarehouse -f scripts/setup_all.sql

echo Database setup complete!
pause

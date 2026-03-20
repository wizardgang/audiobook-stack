@echo off
echo Syncing local abogen source modifications into the tts-node docker context...

:: Remove old bundled copy
rmdir /S /Q abogen_src 2>NUL

:: Create new folder structure
mkdir abogen_src\abogen

:: Copy python module, configuration, and docs
xcopy /S /E /Y /I ..\abogen\* abogen_src\abogen\
copy /Y ..\pyproject.toml abogen_src\
copy /Y ..\README.md abogen_src\
copy /Y ..\.dockerignore .dockerignore

echo Sync complete! The local abogen changes are now bundled in the abogen_src directory.
echo You can now run your docker compose build as normal.

@echo off
REM Windows shim so typing `make <target>` at the repo root will forward to `python -m tasks <target>`
python -m tasks %*

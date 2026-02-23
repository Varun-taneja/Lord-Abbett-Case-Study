@echo off
if "%1"=="install" pip install -r requirements.txt
if "%1"=="pipeline" python data_validation.py
if "%1"=="app" streamlit run app.py
if "%1"=="clean" del summary_output.csv
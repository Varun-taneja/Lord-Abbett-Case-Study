.PHONY: install run-pipeline run-app clean

# Installs the required Python packages
install:
	pip install -r requirements.txt

# Runs the backend ETL pipeline in the terminal
run-pipeline:
	python data_validation.py

# Launches the Streamlit front-end web app
run-app:
	streamlit run app.py

# Cleans up generated files and Python caches
clean:
	rm -rf __pycache__
	rm -f summary_output.csv
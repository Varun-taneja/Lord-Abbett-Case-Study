import pandas as pd
import numpy as np
import logging
import sys

# Configure enterprise-grade logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def extract_data(pos_file: str, prc_file: str, prev_file: str) -> tuple:
    """Extracts raw data from CSV files and includes basic error handling."""
    logging.info("Starting Data Extraction...")
    try:
        pos = pd.read_csv(pos_file)
        prc = pd.read_csv(prc_file)
        prev = pd.read_csv(prev_file)
        logging.info("Successfully extracted all data files.")
        return pos, prc, prev
    except FileNotFoundError as e:
        logging.error(f"Critical Error: Missing file - {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error during extraction: {e}")
        sys.exit(1)

def transform_data(pos: pd.DataFrame, prc: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    """Applies business logic, fallbacks, exposure math, and outlier detection."""
    logging.info("Starting Data Transformation...")
    
    # 1. Joins
    df = pd.merge(pos, prc[['Ticker', 'Price']], on='Ticker', how='left')
    df.rename(columns={'Price': 'Cur_Prc'}, inplace=True)

    df = pd.merge(df, prev[['Ticker', 'Price']], on='Ticker', how='left')
    df.rename(columns={'Price': 'Prev_Prc'}, inplace=True)

    # 2. Fallback Logic
    df['Miss_Prc'] = df['Cur_Prc'].isna()
    df['Fin_Prc'] = df['Cur_Prc'].fillna(df['Prev_Prc'])
    df['Used_Prev'] = df['Miss_Prc'] & df['Fin_Prc'].notna()

    # 3. Exposure Calculation (with safety check for missing quantities)
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce').fillna(0)
    df['Exp'] = df['Qty'] * df['Fin_Prc']

    # 4. Z-Score Outlier Detection
    mean_p = df['Fin_Prc'].mean()
    std_p = df['Fin_Prc'].std()

    if pd.isna(std_p) or std_p == 0:
        logging.warning("Standard deviation is 0. Z-scores set to 0.0 to prevent division by zero.")
        df['Z'] = 0.0
    else:
        df['Z'] = (df['Fin_Prc'] - mean_p) / std_p

    df['Outlier'] = df['Z'].abs() > 3
    
    # Count outliers for the logs
    outlier_count = df['Outlier'].sum()
    logging.info(f"Transformation complete. Detected {outlier_count} outliers.")
    
    return df

def load_data(df: pd.DataFrame, out_file: str) -> None:
    """Filters columns and exports the final dataset."""
    logging.info(f"Loading final summary to {out_file}...")
    cols = ['Ticker', 'Qty', 'Cur_Prc', 'Prev_Prc', 'Fin_Prc', 'Miss_Prc', 'Used_Prev', 'Exp', 'Z', 'Outlier']
    
    try:
        res = df[cols]
        res.to_csv(out_file, index=False)
        logging.info("Pipeline execution completed successfully.")
    except KeyError as e:
        logging.error(f"Transformation failed. Missing expected column: {e}")
        sys.exit(1)

def run_pipeline():
    """Main execution function coordinating the ETL steps."""
    pos, prc, prev = extract_data('positions.csv', 'prices.csv', 'prices_prev.csv')
    transformed_df = transform_data(pos, prc, prev)
    load_data(transformed_df, 'summary_output.csv')

if __name__ == "__main__":
    run_pipeline()
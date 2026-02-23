import streamlit as st
import pandas as pd
from data_validation import extract_data, transform_data

st.set_page_config(page_title="Lord Abbett Price Validation", layout="wide")

st.title("Lord Abbett: Price Data Validation Pipeline")
st.write("Upload the daily position and price feeds to generate the exposure and outlier summary.")

col1, col2, col3 = st.columns(3)
with col1:
    pos_file = st.file_uploader("Upload positions.csv", type=['csv'])
with col2:
    prc_file = st.file_uploader("Upload prices.csv", type=['csv'])
with col3:
    prev_file = st.file_uploader("Upload prices_prev.csv", type=['csv'])

if pos_file and prc_file and prev_file:
    if st.button("Run Validation Pipeline"):
        with st.spinner("Processing data..."):
            try:
                # Step 1: Extract
                pos, prc, prev = extract_data(pos_file, prc_file, prev_file)
                
                # Step 2: Transform
                summary_df = transform_data(pos, prc, prev)
                
                st.success("Pipeline executed successfully!")
                
                # Display the data visually
                st.subheader("Final Output Summary")
                st.dataframe(summary_df, use_container_width=True)
                
                # Step 3: Load (Provide Download Button)
                csv = summary_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Summary as CSV",
                    data=csv,
                    file_name='summary_output.csv',
                    mime='text/csv',
                )
            except Exception as e:
                st.error(f"An error occurred during processing: {e}")
import os
import glob
import gspread
import json
import time
import logging
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

# Config logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_latest_file(extension='xls', directory='.'):
    """Get the most recently modified file with a given extension."""
    files = glob.glob(os.path.join(directory, f'*.{extension}'))
    if not files:
        logging.warning("No files found with the specified extension.")
        return None
    return max(files, key=os.path.getmtime)

def retry_api_call(func, retries=3, delay=2):
    """Retry API calls on 500 errors."""
    for i in range(retries):
        try:
            return func()
        except HttpError as error:
            if hasattr(error, "resp") and error.resp.status == 500:
                logging.warning(f"APIError 500 encountered. Retrying {i + 1}/{retries}...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries reached.")

def process_excel_data(input_file):
    """Load Excel, process data according to new logic."""
    logging.info("Processing Excel file...")
    
    # Read Excel with skiprows=10 as per your working logic
    df = pd.read_excel(input_file, skiprows=10, header=0)
    
    # Remove rows containing "Total Filial:" or "Total Geral:" - EXACTLY as in your working code
    df = df[
        ~df.astype(str)
          .apply(lambda row: row.str.contains(
              r'Total Filial:|Total Geral:',
              regex=True,
              na=False
          ).any(), axis=1)
    ]
    
    # List ALL columns that exist in the DataFrame (for debugging)
    logging.info(f"Original columns: {list(df.columns)}")
    
    # Drop specific columns - using EXACTLY the same columns from your working code
    columns_to_drop = [
        'Unnamed: 0', 'Núm.\nVenda', 'Unnamed: 5', 'Unnamed: 6', 
        'Cond.Pagto', 'Unnamed: 7', 'Tipo', 'Unnamed: 9', 'Unnamed: 10', 
        'Tele', 'Unnamed: 12', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15',
        'Emissão', 'Unnamed: 17', 'Unnamed: 18', 'ECF', 'Unnamed: 21', 
        'Modelo', 'Unnamed: 23', 'Unnamed: 24', 'Unnamed: 27', 'Unnamed: 28',
        'Unnamed: 29', 'Vend. Dev.', 'Unnamed: 31', 'Unnamed: 32', 'Vend.',
        'Unnamed: 34', 'Unnamed: 35', 'Unnamed: 38', 'Unnamed: 39', 
        'Unnamed: 41', 'Unnamed: 42', 'Unnamed: 44', 'Unnamed: 45', 
        'Vlr. Reemb.', 'Unnamed: 47', 'Unnamed: 48'
    ]
    
    # Filter to only drop columns that actually exist
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    logging.info(f"Dropping columns: {existing_columns_to_drop}")
    
    df = df.drop(columns=existing_columns_to_drop, errors="ignore")
    
    # Check what columns remain after dropping
    logging.info(f"Columns after dropping: {list(df.columns)}")
    
    # Check if we have enough columns to rename
    if len(df.columns) < 10:
        logging.error(f"Expected at least 10 columns, but got {len(df.columns)}")
        logging.info(f"Current columns: {list(df.columns)}")
        return df
    
    # Rename columns - using EXACTLY the same renaming logic from your working code
    rename_dict = {
        df.columns[0]: 'Núm. Venda',
        df.columns[1]: 'Filial',
        df.columns[2]: 'Hora',
        df.columns[3]: 'Documento Fiscal',
        df.columns[4]: 'Cliente',
        df.columns[5]: 'Valor Bruto',
        df.columns[6]: '% Desconto',
        df.columns[7]: 'Valor Desconto',
        df.columns[8]: 'Valor Líquido',
        df.columns[9]: 'Total Líquido',
    }
    
    logging.info(f"Renaming columns: {rename_dict}")
    df = df.rename(columns=rename_dict)
    
    # Convert and format time column - EXACTLY as in your working code
    try:
        df['Hora'] = pd.to_datetime(df['Hora'], errors='coerce')
        df['Hora'] = df['Hora'].dt.strftime('%H:%M:%S')  # Format: hh:mm:ss
    except Exception as e:
        logging.error(f"Error processing Hora column: {e}")
    
    # Now keep only the 10 columns we want
    columns_to_keep = [
        'Núm. Venda', 'Filial', 'Hora', 'Documento Fiscal', 'Cliente',
        'Valor Bruto', '% Desconto', 'Valor Desconto', 'Valor Líquido', 
        'Total Líquido'
    ]
    
    # Only keep columns that exist (in case some were dropped earlier)
    existing_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
    df = df[existing_columns_to_keep]
    
    # Reset index
    df = df.reset_index(drop=True)
    
    logging.info(f"Finished processing. Rows remaining: {len(df)}")
    logging.info(f"Final columns: {list(df.columns)}")
    
    return df

'''def convert_pandas_to_sheets_format(df):
    """Convert pandas DataFrame to a format suitable for Google Sheets."""
    # Replace NaN with empty strings
    df = df.fillna("")
    
    # Convert all columns to string to handle mixed types and Timestamp objects
    df = df.astype(str)
    
    # Get headers and values
    headers = df.columns.tolist()
    values = df.values.tolist()
    
    # Combine headers and values
    return [headers] + values'''

def convert_pandas_to_sheets_format(df):
    """Convert pandas DataFrame to a format suitable for Google Sheets."""
    # Replace NaN with empty strings
    df = df.fillna("")
    
    # Process each cell to clean .0 from whole numbers
    def clean_value(val):
        if isinstance(val, (int, np.integer)):
            return val
        if isinstance(val, (float, np.floating)):
            if val.is_integer():
                return int(val)
            return val
        if isinstance(val, str):
            # Remove .0 from end of string numbers
            if val.endswith('.0'):
                try:
                    num = float(val)
                    if num.is_integer():
                        return int(num)
                except:
                    pass
        return val
    
    # Apply cleaning
    cleaned_data = []
    for _, row in df.iterrows():
        cleaned_row = [clean_value(cell) for cell in row]
        cleaned_data.append(cleaned_row)
    
    return [df.columns.tolist()] + cleaned_data

def update_google_sheet(df, sheet_id, worksheet_name="APP_TRIER"):
    """Update Google Sheet with the processed data"""
    logging.info("Checking Google credentials environment variable...")
    creds_json = os.getenv("GSA_CREDENTIALS")
    if creds_json is None:
        logging.error("Google credentials not found in environment variables.")
        return

    creds_dict = json.loads(creds_json)
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(creds)
    
    # Open spreadsheet and worksheet
    try:
        spreadsheet = client.open_by_key(sheet_id)
        sheet = spreadsheet.worksheet(worksheet_name)
    except Exception as e:
        logging.error(f"Error accessing spreadsheet: {e}")
        return

    # Prepare data in Google Sheets compatible format
    logging.info("Preparing data for Google Sheets...")
    rows = convert_pandas_to_sheets_format(df)

    # Clear sheet and update
    logging.info("Clearing existing data...")
    sheet.clear()
    logging.info(f"Uploading {len(rows)} rows of data...")
    retry_api_call(lambda: sheet.update(rows, value_input_option='USER_ENTERED'))
    logging.info("Google Sheet updated successfully.")

def main():
    download_dir = '/home/runner/work/app_trier/app_trier/'
    latest_file = get_latest_file(directory=download_dir)
    sheet_id = os.getenv("sheet_id")

    if latest_file:
        logging.info(f"Loaded file: {latest_file}")
        try:
            # Process the Excel file
            processed_df = process_excel_data(latest_file)
            
            if processed_df.empty:
                logging.warning("Processed DataFrame is empty. Skipping sheet update.")
                return
            
            # Display sample data for verification
            logging.info(f"Sample data (first 5 rows):")
            for i in range(min(5, len(processed_df))):
                logging.info(f"Row {i}: {dict(zip(processed_df.columns, processed_df.iloc[i]))}")
            
            # Optional: Save to local file for debugging
            # processed_df.to_excel("debug_output.xlsx", index=False)
            # logging.info("Saved debug output to debug_output.xlsx")
            
            # Update Google Sheet
            update_google_sheet(processed_df, sheet_id, "APP_TRIER")
            
        except Exception as e:
            logging.error(f"Error processing file: {e}")
            import traceback
            traceback.print_exc()
            return
    else:
        logging.warning("No new files to process.")

if __name__ == "__main__":
    main()

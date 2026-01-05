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
from openpyxl.styles import Font

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
    
    # Read Excel with skiprows=10 as per new logic
    df = pd.read_excel(input_file, skiprows=10, header=0)
    
    # Remove rows containing "Total Filial:" or "Total Geral:"
    df = df[
        ~df.astype(str)
          .apply(lambda row: row.str.contains(
              r'Total Filial:|Total Geral:',
              regex=True,
              na=False
          ).any(), axis=1)
    ]
    
    # Drop specific columns
    columns_to_drop = [
        'Unnamed: 0', 'Unnamed: 1', 'Unnamed: 3', 'Unnamed: 6', 'Unnamed: 7',
        'Unnamed: 8', 'Tipo', 'Unnamed: 10', 'Unnamed: 11', 'Tele',
        'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15', 'Emissão', 'Unnamed: 18',
        'Unnamed: 19', 'Unnamed: 22', 'Modelo', 'Unnamed: 24', 'Unnamed: 25',
        'Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30', 'Vend. Dev.',
        'Unnamed: 32', 'Unnamed: 33', 'Unnamed: 35', 'Unnamed: 36', '% Desc.',
        'Unnamed: 39', 'Unnamed: 40', 'Unnamed: 42', 'Unnamed: 43',
        'Unnamed: 45', 'Unnamed: 46', 'Vlr. Reemb.', 'Unnamed: 48',
        'Unnamed: 49', 'Núm.\nVenda', 'Cond.Pagto', 'ECF'
    ]
    
    df = df.drop(columns=columns_to_drop, errors="ignore")
    
    # Rename columns
    df = df.rename(columns={
        df.columns[0]: 'Filial',
        df.columns[1]: 'Emissão',
        df.columns[2]: 'Hora',
        df.columns[3]: 'Documento Fiscal',
        df.columns[4]: 'Cliente',
        df.columns[5]: 'Vendedor',
        df.columns[6]: 'Valor Bruto',
        df.columns[7]: 'Valor Desconto',
        df.columns[8]: 'Valor Líquido',
        df.columns[9]: 'Total Líquido',
    })
    
    # Convert and format date/time columns
    df['Emissão'] = pd.to_datetime(df['Emissão'], errors='coerce')
    df['Hora'] = pd.to_datetime(df['Hora'], errors='coerce')
    
    # Format the columns
    df['Emissão'] = df['Emissão'].dt.strftime('%d/%m/%Y')  # Format: dd/mm/yyyy
    df['Hora'] = df['Hora'].dt.strftime('%H:%M:%S')  # Format: hh:mm:ss
    
    # Reset index
    df = df.reset_index(drop=True)
    
    logging.info(f"Finished processing. Rows remaining: {len(df)}")
    logging.info(f"Columns: {list(df.columns)}")
    
    return df

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

    # Prepare data
    logging.info("Preparing data for Google Sheets...")
    df = df.fillna("")  # Ensure no NaN values
    rows = [df.columns.tolist()] + df.values.tolist()

    # Clear sheet and update
    logging.info("Clearing existing data...")
    sheet.clear()
    logging.info("Uploading new data...")
    retry_api_call(lambda: sheet.update(rows))
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
            logging.info(f"Sample data:\n{processed_df.head()}")
            
            # Update Google Sheet
            update_google_sheet(processed_df, sheet_id, "APP_TRIER")
            
        except Exception as e:
            logging.error(f"Error processing file: {e}")
            return
    else:
        logging.warning("No new files to process.")

if __name__ == "__main__":
    main()

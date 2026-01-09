import os
import json
import logging
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ================= CONFIG =================

VALUE_TOLERANCE = 0.15      # R$

SOURCE_SHEET_ID = os.getenv("sheet_id")
CREDS_JSON = os.getenv("GSA_CREDENTIALS")

APP_SHEET = "APP"
APP_TRIER_SHEET = "APP_TRIER"
OUTPUT_SHEET = "APPXTRIER"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= HELPERS =================

def connect_sheet():
    creds_dict = json.loads(CREDS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SOURCE_SHEET_ID)


def read_worksheet_as_df(sheet, name):
    ws = sheet.worksheet(name)
    return pd.DataFrame(ws.get_all_records())


def clear_and_write(sheet, name, df):
    try:
        ws = sheet.worksheet(name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=name, rows=1000, cols=20)

    ws.update([df.columns.tolist()] + df.fillna("").values.tolist())


def parse_brl_currency(value):
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)

    return float(
        value.replace("R$", "")
             .replace(" ", "")
             .replace(".", "")
             .replace(",", ".")
    )

# ================= MAIN LOGIC =================

def reconcile_app_vs_trier(sheet):
    logging.info("Reading APP...")
    df_app = read_worksheet_as_df(sheet, APP_SHEET)

    logging.info("Reading APP_TRIER...")
    df_trier = read_worksheet_as_df(sheet, APP_TRIER_SHEET)

    # Normalize columns
    df_app.columns = df_app.columns.str.strip()
    df_trier.columns = df_trier.columns.str.strip()

    # Filter valid payments
    df_app = df_app[df_app["Pagamento"].isin(["Pix", "Cartão"])].copy()

    # Parse APP values
    df_app["APP_VALOR_NUM"] = df_app["Valor"].apply(parse_brl_currency)

    results = []

    for _, trier_row in df_trier.iterrows():
        filial = trier_row["Filial"]
        total_liquido = parse_brl_currency(trier_row["Total Líquido"])

        # Value-based candidates only
        candidates = df_app.copy()

        if candidates.empty:
            results.append(build_no_match_row(trier_row))
            continue

        candidates["VALOR_DIFF_ABS"] = (
            candidates["APP_VALOR_NUM"] - total_liquido
        ).abs()

        candidates = candidates[candidates["VALOR_DIFF_ABS"] <= VALUE_TOLERANCE]

        if candidates.empty:
            results.append(build_no_match_row(trier_row))
            continue

        # Pick closest value
        match = candidates.sort_values("VALOR_DIFF_ABS").iloc[0]

        status = classify_status(match["VALOR_DIFF_ABS"])

        results.append({
            "Filial": filial,
            "Núm. Venda": trier_row["Núm. Venda"],
            "Cliente": trier_row["Cliente"],
            "Criado em (APP)": match.get("Criado em", ""),
            "Hora (Trier)": trier_row["Hora"],
            "Valor Venda APP": round(match["APP_VALOR_NUM"], 2),
            "Total Líquido (Trier)": total_liquido,
            "Status": status
        })

    return pd.DataFrame(results)


def classify_status(diff_abs):
    if diff_abs == 0:
        return "OK"
    if diff_abs <= VALUE_TOLERANCE:
        return "OK (AJUSTE)"
    return "VALOR DIVERGENTE"


def build_no_match_row(trier_row):
    return {
        "Filial": trier_row["Filial"],
        "Núm. Venda": trier_row["Núm. Venda"],
        "Cliente": trier_row["Cliente"],
        "Criado em (APP)": "",
        "Hora (Trier)": trier_row["Hora"],
        "Valor Venda APP": "",
        "Total Líquido (Trier)": parse_brl_currency(trier_row["Total Líquido"]),
        "Status": "SEM CORRESPONDÊNCIA"
    }

# ================= ENTRYPOINT =================

def main():
    sheet = connect_sheet()
    df_result = reconcile_app_vs_trier(sheet)
    clear_and_write(sheet, OUTPUT_SHEET, df_result)
    logging.info("APPXTRIER reconciliation completed successfully.")


if __name__ == "__main__":
    main()

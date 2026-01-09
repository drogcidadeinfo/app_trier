import os
import json
import logging
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ================= CONFIG =================

VALUE_TOLERANCE = 0.15      # R$
TIME_TOLERANCE_MIN = 5     # minutes

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


def time_to_minutes(t):
    return t.hour * 60 + t.minute


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
    df_app = df_app[df_app["Pagamento"].isin(["PIX", "Cartão"])].copy()

    # Parse APP values
    df_app["APP_VALOR_NUM"] = df_app["Valor"].apply(parse_brl_currency)

    # Parse APP time only
    df_app["APP_TIME"] = pd.to_datetime(
        df_app["Criado em"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce"
    ).dt.time

    results = []

    for _, trier_row in df_trier.iterrows():
        filial = trier_row["Filial"]
        total_liquido = parse_brl_currency(trier_row["Total Líquido"])
        hora_trier = trier_row["Hora"]

        candidates = df_app[df_app["Filial"] == filial].copy()

        if candidates.empty:
            results.append(build_no_match_row(trier_row))
            continue

        # Value comparison
        candidates["VALOR_DIFF_ABS"] = (
            candidates["APP_VALOR_NUM"] - total_liquido
        ).abs()

        candidates = candidates[candidates["VALOR_DIFF_ABS"] <= VALUE_TOLERANCE]

        if candidates.empty:
            results.append(build_no_match_row(trier_row))
            continue

        # Time comparison (minutes only)
        try:
            trier_time = datetime.strptime(hora_trier, "%H:%M:%S").time()
        except Exception:
            results.append(build_no_match_row(trier_row))
            continue

        candidates["TIME_DIFF_MIN"] = candidates["APP_TIME"].apply(
            lambda t: abs(time_to_minutes(t) - time_to_minutes(trier_time))
            if pd.notna(t) else 9999
        )

        candidates = candidates[candidates["TIME_DIFF_MIN"] <= TIME_TOLERANCE_MIN]

        if candidates.empty:
            results.append(build_no_match_row(trier_row))
            continue

        # Pick closest time
        match = candidates.sort_values("TIME_DIFF_MIN").iloc[0]

        status = classify_status(match["VALOR_DIFF_ABS"])

        results.append({
            "Filial": filial,
            "Núm. Venda": trier_row["Núm. Venda"],
            "Cliente": trier_row["Cliente"],
            "Hora": hora_trier,
            "Total Líquido": total_liquido,
            "Valor Venda APP": round(match["APP_VALOR_NUM"], 2),
            "Status": status
        })

    return pd.DataFrame(results)


def classify_status(diff_abs):
    if diff_abs == 0:
        return "MATCH"
    if diff_abs <= VALUE_TOLERANCE:
        return "MATCH (AJUSTE)"
    return "VALOR DIVERGENTE"


def build_no_match_row(trier_row):
    return {
        "Filial": trier_row["Filial"],
        "Núm. Venda": trier_row["Núm. Venda"],
        "Cliente": trier_row["Cliente"],
        "Hora": trier_row["Hora"],
        "Total Líquido": parse_brl_currency(trier_row["Total Líquido"]),
        "Valor Venda APP": "",
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

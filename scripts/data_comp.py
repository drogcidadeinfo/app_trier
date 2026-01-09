import os
import logging
import pandas as pd
from datetime import datetime, timedelta
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ================= HELPERS =================

def connect_sheet():
    creds_dict = json.loads(CREDS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SOURCE_SHEET_ID)


def read_worksheet_as_df(sheet, name):
    ws = sheet.worksheet(name)
    data = ws.get_all_records()
    return pd.DataFrame(data)


def clear_and_write(sheet, name, df):
    try:
        ws = sheet.worksheet(name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=name, rows=1000, cols=30)

    ws.update([df.columns.tolist()] + df.fillna("").values.tolist())


def parse_app_datetime(value):
    return datetime.strptime(value, "%d/%m/%Y %H:%M:%S")


def parse_trier_time(hora, reference_date):
    t = datetime.strptime(hora, "%H:%M:%S").time()
    return datetime.combine(reference_date.date(), t)

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

    # Clean columns
    df_app.columns = df_app.columns.str.strip()
    df_trier.columns = df_trier.columns.str.strip()

    # Filter APP payments
    df_app = df_app[df_app["Pagamento"].isin(["PIX", "Cartão"])]

    # Parse datetimes
    df_app["APP_DATETIME"] = df_app["Criado em"].apply(parse_app_datetime)

    results = []

    for _, trier_row in df_trier.iterrows():
        filial = trier_row["Filial"]
        total_liquido = parse_brl_currency(trier_row["Total Líquido"])
        hora_trier = trier_row["Hora"]

        # Filter APP by filial
        candidates = df_app[df_app["Filial"] == filial].copy()

        if candidates.empty:
            results.append(build_no_match_row(trier_row))
            continue

        # Build datetime for Trier
        ref_date = candidates.iloc[0]["APP_DATETIME"]
        trier_datetime = parse_trier_time(hora_trier, ref_date)

        # Compute diffs
        candidates["APP_VALOR_NUM"] = candidates["Valor"].apply(parse_brl_currency)

        candidates["VALOR_DIFF"] = candidates["APP_VALOR_NUM"] - total_liquido
        candidates["VALOR_DIFF_ABS"] = candidates["VALOR_DIFF"].abs()

        candidates["VALOR_DIFF_ABS"] = candidates["VALOR_DIFF"].abs()
        candidates["TIME_DIFF_MIN"] = (
            candidates["APP_DATETIME"] - trier_datetime
        ).abs().dt.total_seconds() / 60

        # Filter by time tolerance
        candidates = candidates[candidates["TIME_DIFF_MIN"] <= TIME_TOLERANCE_MIN]

        if candidates.empty:
            results.append(build_no_match_row(trier_row))
            continue

        # Pick closest time match
        match = candidates.sort_values("TIME_DIFF_MIN").iloc[0]

        status = classify_status(match["VALOR_DIFF_ABS"])

        results.append({
            "Filial": filial,
            "Núm. Venda": trier_row["Núm. Venda"],
            "Cliente": trier_row["Cliente"],
            "Hora": hora_trier,
            "Total Líquido": total_liquido,
            "APP Valor": match["Valor"],
            "APP Pagamento": match["Pagamento"],
            "APP Criado em": match["Criado em"],
            "Diferença (R$)": round(match["VALOR_DIFF"], 2),
            "Diferença Absoluta": round(match["VALOR_DIFF_ABS"], 2),
            "Diferença Aceita": "SIM" if match["VALOR_DIFF_ABS"] <= VALUE_TOLERANCE else "NÃO",
            "Diferença de Tempo (min)": int(match["TIME_DIFF_MIN"]),
            "Tempo Aceito": "SIM",
            "Status": status
        })

    return pd.DataFrame(results)


def classify_status(valor_diff_abs):
    if valor_diff_abs == 0:
        return "MATCH"
    if valor_diff_abs <= VALUE_TOLERANCE:
        return "MATCH (AJUSTE)"
    return "VALOR DIVERGENTE"


def build_no_match_row(trier_row):
    return {
        "Filial": trier_row["Filial"],
        "Núm. Venda": trier_row["Núm. Venda"],
        "Cliente": trier_row["Cliente"],
        "Hora": trier_row["Hora"],
        "Total Líquido": trier_row["Total Líquido"],
        "APP Valor": "",
        "APP Pagamento": "",
        "APP Criado em": "",
        "Diferença (R$)": "",
        "Diferença Absoluta": "",
        "Diferença Aceita": "",
        "Diferença de Tempo (min)": "",
        "Tempo Aceito": "",
        "Status": "SEM CORRESPONDÊNCIA"
    }

# ================= ENTRYPOINT =================

def main():
    sheet = connect_sheet()
    df_result = reconcile_app_vs_trier(sheet)
    clear_and_write(sheet, OUTPUT_SHEET, df_result)
    logging.info("APPXTRIER reconciliation completed.")


if __name__ == "__main__":
    import json
    main()

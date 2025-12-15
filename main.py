from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters
import pandas as pd

TOKEN = 8538966637:AAGadw

async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = await update.message.document.get_file()
    await file.download_to_drive("input.xlsx")

    df = pd.read_excel("input.xlsx")

    df["CALL_DATETIME"] = pd.to_datetime(
        df["FULL_DATE"].astype(str) + " " + df["CALL_TIME"].astype(str),
        errors="coerce"
    )

    df_sorted = df.sort_values("CALL_DATETIME")

    writer = pd.ExcelWriter("gamil_analysis.xlsx", engine="openpyxl")

    df.to_excel(writer, sheet_name="RAW_DATA", index=False)
    df_sorted.to_excel(writer, sheet_name="CALLS_TIMELINE", index=False)

    loc = df_sorted.dropna(subset=["LATITUDE", "LONGITUDE"])
    last_seen = (
        loc.groupby(["LATITUDE", "LONGITUDE", "SITE_ADDRESS"], dropna=False)
        .last()
        .reset_index()
    )
    last_seen["GOOGLE_MAPS"] = last_seen.apply(
        lambda r: f"https://www.google.com/maps?q={r['LATITUDE']},{r['LONGITUDE']}",
        axis=1
    )
    last_seen.to_excel(writer, sheet_name="LOCATION_LAST_SEEN", index=False)

    writer.close()

    await update.message.reply_document(open("gamil_analysis.xlsx", "rb"))

app = ApplicationBuilder().token(TOKEN).build()
app.add_handler(MessageHandler(filters.Document.ALL, handle_excel))
app.run_polling()

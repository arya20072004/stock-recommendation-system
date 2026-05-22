# sector_index_builder.py
"""
Builds equal-weighted daily sector return indices from Nifty 500 universe.
Stores results in MongoDB sector_indices collection.

Run once after data_collector.py, then weekly via APScheduler.
Fixes sector momentum for OilGas, Metals, CementInfra, PowerUtilities
which have < 4 peers in the Nifty 50 universe.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()
uri = os.getenv("MONGO_URI")
MONGO_URI   = os.getenv("MONGO_URI", uri)
HISTORY_YEARS = 5
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# Extended sector map — Nifty 500 representatives per sector
# Add as many liquid tickers as needed; self-exclusion handles overlap with Nifty 50
NIFTY500_SECTOR_MAP: dict[str, list[str]] = {
    "AutomobileAndAutoComponents": [
        "ARE&M.NS",
        "APOLLOTYRE.NS",
        "ASAHIINDIA.NS",
        "ATHERENERG.NS",
        "BAJAJ-AUTO.NS",
        "BALKRISIND.NS",
        "BELRISE.NS",
        "BHARATFORG.NS",
        "BOSCHLTD.NS",
        "CEATLTD.NS",
        "CIEINDIA.NS",
        "CRAFTSMAN.NS",
        "EICHERMOT.NS",
        "ENDURANCE.NS",
        "EXIDEIND.NS",
        "FORCEMOT.NS",
        "GABRIEL.NS",
        "HEROMOTOCO.NS",
        "HYUNDAI.NS",
        "JBMA.NS",
        "JKTYRE.NS",
        "M&M.NS",
        "MARUTI.NS",
        "MINDACORP.NS",
        "MSUMI.NS",
        "MOTHERSON.NS",
        "OLAELEC.NS",
        "OLECTRA.NS",
        "RKFORGE.NS",
        "SCHAEFFLER.NS",
        "SONACOMS.NS",
        "TMPV.NS",
        "TVSMOTOR.NS",
        "TENNIND.NS",
        "TIINDIA.NS",
        "UNOMINDA.NS",
        "ZFCVINDIA.NS"
    ],

    "CapitalGoods": [
        "ABB.NS",
        "AIAENG.NS",
        "APLAPOLLO.NS",
        "ACE.NS",
        "CPPLUS.NS",
        "APARINDS.NS",
        "ASHOKLEY.NS",
        "ASTRAL.NS",
        "BEML.NS",
        "BDL.NS",
        "BEL.NS",
        "BHEL.NS",
        "CGPOWER.NS",
        "CARBORUNIV.NS",
        "COCHINSHIP.NS",
        "CUMMINSIND.NS",
        "DATAPATTNS.NS",
        "ELECON.NS",
        "ELGIEQUIP.NS",
        "EMMVEE.NS",
        "ESCORTS.NS",
        "FINCABLES.NS",
        "GVT&D.NS",
        "GALLANTT.NS",
        "GRSE.NS",
        "GPIL.NS",
        "GRAPHITE.NS",
        "HEG.NS",
        "HBLENGINE.NS",
        "HAL.NS",
        "POWERINDIA.NS",
        "HONAUT.NS",
        "INOXWIND.NS",
        "JINDALSAW.NS",
        "JWL.NS",
        "JYOTICNC.NS",
        "KEI.NS",
        "KAYNES.NS",
        "KIRLOSENG.NS",
        "MAZDOCK.NS",
        "POLYCAB.NS",
        "PREMIERENE.NS",
        "PTCIL.NS",
        "RRKABEL.NS",
        "RHIM.NS",
        "SCHNEIDER.NS",
        "SHYAMMETL.NS",
        "ENRIN.NS",
        "SIEMENS.NS",
        "SUPREMEIND.NS",
        "SUZLON.NS",
        "SYRMA.NS",
        "TEGA.NS",
        "THERMAX.NS",
        "TIMKEN.NS",
        "TITAGARH.NS",
        "TARIL.NS",
        "TRITURBINE.NS",
        "USHAMART.NS",
        "WAAREEENER.NS",
        "WELCORP.NS",
        "ZENTEC.NS"
    ],

    "Chemicals": [
        "AARTIIND.NS",
        "ANURAS.NS",
        "ATUL.NS",
        "BAYERCROP.NS",
        "CHAMBLFERT.NS",
        "COROMANDEL.NS",
        "CLEAN.NS",
        "DEEPAKFERT.NS",
        "DEEPAKNTR.NS",
        "FACT.NS",
        "FLUOROCHEM.NS",
        "HSCL.NS",
        "JUBLINGREA.NS",
        "LINDEINDIA.NS",
        "NAVINFLUOR.NS",
        "PCBL.NS",
        "PIIND.NS",
        "PARADEEP.NS",
        "PIDILITIND.NS",
        "SRF.NS",
        "SUMICHEM.NS",
        "SPLPETRO.NS",
        "SWANCORP.NS",
        "TATACHEM.NS",
        "UPL.NS"
    ],

    "Construction": [
        "AFCONS.NS",
        "CEMPRO.NS",
        "ENGINERSIN.NS",
        "IRB.NS",
        "IRCON.NS",
        "KPIL.NS",
        "KEC.NS",
        "LT.NS",
        "NBCC.NS",
        "NCC.NS",
        "RITES.NS",
        "RVNL.NS",
        "TECHNOE.NS"
    ],

    "ConstructionMaterials": [
        "ACC.NS",
        "AMBUJACEM.NS",
        "DALBHARAT.NS",
        "GRASIM.NS",
        "INDIACEM.NS",
        "JKCEMENT.NS",
        "JSWCEMENT.NS",
        "NUVOCO.NS",
        "SHREECEM.NS",
        "RAMCOCEM.NS",
        "ULTRACEMCO.NS"
    ],

    "ConsumerDurables": [
        "AMBER.NS",
        "ASIANPAINT.NS",
        "BATAINDIA.NS",
        "BERGEPAINT.NS",
        "BLUESTARCO.NS",
        "CROMPTON.NS",
        "DIXON.NS",
        "HAVELLS.NS",
        "JSWDULUX.NS",
        "KAJARIACER.NS",
        "KALYANKJIL.NS",
        "LGEINDIA.NS",
        "PGEL.NS",
        "TITAN.NS",
        "VOLTAS.NS",
        "WHIRLPOOL.NS"
    ],

    "ConsumerServices": [
        "ABFRL.NS",
        "ABLBL.NS",
        "DMART.NS",
        "BLS.NS",
        "FIRSTCRY.NS",
        "CARTRADE.NS",
        "CHALET.NS",
        "DEVYANI.NS",
        "EIHOTEL.NS",
        "ETERNAL.NS",
        "NYKAA.NS",
        "ITCHOTELS.NS",
        "INDIAMART.NS",
        "INDHOTEL.NS",
        "IRCTC.NS",
        "NAUKRI.NS",
        "JUBLFOOD.NS",
        "THELEELA.NS",
        "LEMONTREE.NS",
        "SAPPHIRE.NS",
        "SWIGGY.NS",
        "TBOTEK.NS",
        "TRAVELFOOD.NS",
        "TRENT.NS",
        "URBANCO.NS",
        "VMM.NS"
    ],

    "Diversified": [
        "3MINDIA.NS",
        "DCMSHRIRAM.NS",
        "GODREJIND.NS"
    ],

    "FastMovingConsumerGoods": [
        "AWL.NS",
        "ABDL.NS",
        "BALRAMCHIN.NS",
        "BIKAJI.NS",
        "BBTC.NS",
        "BRITANNIA.NS",
        "CCL.NS",
        "COLPAL.NS",
        "DOMS.NS",
        "DABUR.NS",
        "EMAMILTD.NS",
        "GILLETTE.NS",
        "GODFRYPHLP.NS",
        "GODREJCP.NS",
        "HINDUNILVR.NS",
        "HONASA.NS",
        "ITC.NS",
        "LTFOODS.NS",
        "MARICO.NS",
        "NESTLEIND.NS",
        "PATANJALI.NS",
        "RADICO.NS",
        "TATACONSUM.NS",
        "UBL.NS",
        "UNITDSPR.NS",
        "VBL.NS",
        "ZYDUSWELL.NS"
    ],

    "FinancialServices": [
        "360ONE.NS",
        "AUBANK.NS",
        "AADHARHFC.NS",
        "AAVAS.NS",
        "ABCAPITAL.NS",
        "ABSLAMC.NS",
        "ANANDRATHI.NS",
        "ANGELONE.NS",
        "APTUS.NS",
        "AIIL.NS",
        "AXISBANK.NS",
        "BSE.NS",
        "BAJFINANCE.NS",
        "BAJAJFINSV.NS",
        "BAJAJHLDNG.NS",
        "BAJAJHFL.NS",
        "BANDHANBNK.NS",
        "BANKBARODA.NS",
        "BANKINDIA.NS",
        "MAHABANK.NS",
        "GROWW.NS",
        "CRISIL.NS",
        "CANFINHOME.NS",
        "CANBK.NS",
        "CANHLIFE.NS",
        "CGCL.NS",
        "CHOICEIN.NS",
        "CHOLAHLDNG.NS",
        "CHOLAFIN.NS",
        "CUB.NS",
        "CAMS.NS",
        "CREDITACC.NS",
        "CENTRALBK.NS",
        "CDSL.NS",
        "FEDERALBNK.NS",
        "FIVESTAR.NS",
        "GICRE.NS",
        "GODIGIT.NS",
        "HDBFS.NS",
        "HDFCAMC.NS",
        "HDFCBANK.NS",
        "HDFCLIFE.NS",
        "HOMEFIRST.NS",
        "HUDCO.NS",
        "ICICIBANK.NS",
        "ICICIGI.NS",
        "ICICIAMC.NS",
        "ICICIPRULI.NS",
        "IDBI.NS",
        "IDFCFIRSTB.NS",
        "IFCI.NS",
        "IIFL.NS",
        "INDIANB.NS",
        "IEX.NS",
        "IOB.NS",
        "IRFC.NS",
        "IREDA.NS",
        "INDUSINDBK.NS",
        "JMFINANCIL.NS",
        "J&KBANK.NS",
        "JIOFIN.NS",
        "KARURVYSYA.NS",
        "KFINTECH.NS",
        "KOTAKBANK.NS",
        "LTF.NS",
        "LICHSGFIN.NS",
        "LICI.NS",
        "M&MFIN.NS",
        "MANAPPURAM.NS",
        "MFSL.NS",
        "MOTILALOFS.NS",
        "MCX.NS",
        "MUTHOOTFIN.NS",
        "NAM-INDIA.NS",
        "NIVABUPA.NS",
        "NUVAMA.NS",
        "PAYTM.NS",
        "POLICYBZR.NS",
        "PINELABS.NS",
        "PIRAMALFIN.NS",
        "POONAWALLA.NS",
        "PFC.NS",
        "PNB.NS",
        "PNBHOUSING.NS",
        "RBLBANK.NS",
        "RECLTD.NS",
        "SBFC.NS",
        "SBICARD.NS",
        "SBILIFE.NS",
        "SAMMAANCAP.NS",
        "SHRIRAMFIN.NS",
        "STARHEALTH.NS",
        "SBIN.NS",
        "SUNDARMFIN.NS",
        "TATACAP.NS",
        "TATAINVEST.NS",
        "NIACL.NS",
        "UCOBANK.NS",
        "UTIAMC.NS",
        "UNIONBANK.NS",
        "YESBANK.NS"
    ],

    "Healthcare": [
        "ABBOTINDIA.NS",
        "ACUTAAS.NS",
        "ANTHEM.NS",
        "AJANTPHARM.NS",
        "ALKEM.NS",
        "APOLLOHOSP.NS",
        "ASTERDM.NS",
        "AUROPHARMA.NS",
        "BIOCON.NS",
        "BLUEJET.NS",
        "CAPLIPOINT.NS",
        "CIPLA.NS",
        "COHANCE.NS",
        "CONCORDBIO.NS",
        "DIVISLAB.NS",
        "LALPATHLAB.NS",
        "DRREDDY.NS",
        "EMCURE.NS",
        "ERIS.NS",
        "FORTIS.NS",
        "GLAND.NS",
        "GLAXO.NS",
        "GLENMARK.NS",
        "MEDANTA.NS",
        "GRANULES.NS",
        "INDGN.NS",
        "IPCALAB.NS",
        "JBCHEPHARM.NS",
        "JUBLPHARMA.NS",
        "KIMS.NS",
        "LAURUSLABS.NS",
        "LUPIN.NS",
        "MANKIND.NS",
        "MAXHEALTH.NS",
        "NATCOPHARM.NS",
        "NEULANDLAB.NS",
        "NH.NS",
        "ONESOURCE.NS",
        "PFIZER.NS",
        "PPLPHARMA.NS",
        "POLYMED.NS",
        "RAINBOW.NS",
        "SAILIFE.NS",
        "SUNPHARMA.NS",
        "SYNGENE.NS",
        "TORNTPHARM.NS",
        "VIJAYA.NS",
        "WOCKPHARMA.NS",
        "ZYDUSLIFE.NS"
    ],

    "InformationTechnology": [
        "AFFLE.NS",
        "BSOFT.NS",
        "MAPMYINDIA.NS",
        "COFORGE.NS",
        "CYIENT.NS",
        "HCLTECH.NS",
        "HEXT.NS",
        "INFY.NS",
        "INTELLECT.NS",
        "IKS.NS",
        "KPITTECH.NS",
        "LTTS.NS",
        "LATENTVIEW.NS",
        "LTM.NS",
        "MPHASIS.NS",
        "NETWEB.NS",
        "NEWGEN.NS",
        "OFSS.NS",
        "PERSISTENT.NS",
        "SAGILITY.NS",
        "SONATSOFTW.NS",
        "TCS.NS",
        "TATAELXSI.NS",
        "TATATECH.NS",
        "TECHM.NS",
        "WIPRO.NS",
        "ZENSARTECH.NS"
    ],

    "MediaEntertainmentAndPublication": [
        "PVRINOX.NS",
        "SAREGAMA.NS",
        "SUNTV.NS",
        "ZEEL.NS"
    ],

    "MetalsAndMining": [
        "ADANIENT.NS",
        "GRAVITA.NS",
        "GMDCLTD.NS",
        "HINDALCO.NS",
        "HINDCOPPER.NS",
        "HINDZINC.NS",
        "JSWSTEEL.NS",
        "JSL.NS",
        "JINDALSTEL.NS",
        "LLOYDSME.NS",
        "NMDC.NS",
        "NSLNISP.NS",
        "NATIONALUM.NS",
        "SAIL.NS",
        "SARDAEN.NS",
        "TATASTEEL.NS",
        "VEDL.NS"
    ],

    "OilGasAndConsumableFuels": [
        "ATGL.NS",
        "AEGISLOG.NS",
        "AEGISVOPAK.NS",
        "BPCL.NS",
        "CASTROLIND.NS",
        "CHENNPETRO.NS",
        "GAIL.NS",
        "HINDPETRO.NS",
        "IOC.NS",
        "IGL.NS",
        "MGL.NS",
        "MRPL.NS",
        "ONGC.NS",
        "OIL.NS",
        "PETRONET.NS",
        "RELIANCE.NS"
    ],

    "PowerUtilities": [
        "ACMESOLAR.NS",
        "ADANIENSOL.NS",
        "ADANIGREEN.NS",
        "ADANIPOWER.NS",
        "CESC.NS",
        "COALINDIA.NS",
        "JSWENERGY.NS",
        "JPPOWER.NS",
        "NHPC.NS",
        "NLCINDIA.NS",
        "NTPCGREEN.NS",
        "NTPC.NS",
        "NAVA.NS",
        "POWERGRID.NS",
        "RPOWER.NS",
        "SJVN.NS",
        "TATAPOWER.NS",
        "TORNTPOWER.NS"
    ],

    "Realty": [
        "ABREL.NS",
        "ANANTRAJ.NS",
        "BRIGADE.NS",
        "DLF.NS",
        "GODREJPROP.NS",
        "LODHA.NS",
        "OBEROIRLTY.NS",
        "PHOENIXLTD.NS",
        "PRESTIGE.NS",
        "SIGNATURE.NS",
        "SOBHA.NS"
    ],

    "Services": [
        "ADANIPORTS.NS",
        "BLUEDART.NS",
        "CONCOR.NS",
        "DELHIVERY.NS",
        "GMRAIRPORT.NS",
        "GESHIP.NS",
        "INDIGO.NS",
        "IGIL.NS",
        "JSWINFRA.NS",
        "MMTC.NS",
        "REDINGTON.NS",
        "SCI.NS",
        "ECLERX.NS"
    ],

    "Telecommunication": [
        "BHARTIARTL.NS",
        "BHARTIHEXA.NS",
        "HFCL.NS",
        "ITI.NS",
        "INDUSTOWER.NS",
        "RAILTEL.NS",
        "TATACOMM.NS",
        "TTML.NS",
        "TEJASNET.NS",
        "IDEA.NS"
    ],

    "Textiles": [
        "PAGEIND.NS",
        "TRIDENT.NS",
        "VTL.NS",
        "WELSPUNLIV.NS"
    ]
}


def build_sector_indices(client: MongoClient) -> None:
    db = client["stock_market_db"]
    start_date = (
        datetime.now(timezone.utc)
        - timedelta(days=365 * HISTORY_YEARS + 30)
    ).strftime("%Y-%m-%d")
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for sector, tickers in NIFTY500_SECTOR_MAP.items():
        logger.info("Building sector index for: %s (%d tickers)", sector, len(tickers))

        peer_returns: dict[str, pd.Series] = {}
        for ticker in tickers:
            try:
                raw = yf.download(
                    ticker, start=start_date, end=end_date,
                    progress=False, auto_adjust=True,
                )
                if raw.empty:
                    continue
                if isinstance(raw.columns, pd.MultiIndex):
                    raw.columns = raw.columns.get_level_values(0)
                close = pd.to_numeric(raw["Close"], errors="coerce").dropna()
                if len(close) < 50:
                    continue
                peer_returns[ticker] = close.pct_change().rename(ticker)
            except Exception as ex:
                logger.warning("Failed to fetch %s: %s", ticker, ex)

        if len(peer_returns) < 2:
            logger.warning("%s: insufficient peers (%d), skipping", sector, len(peer_returns))
            continue

        valid_peer_count = len(peer_returns)

        if valid_peer_count < 5:
            logger.warning(
                "%s: only %d valid peers after download filtering",
                sector,
                valid_peer_count,
            )
            continue
        # Equal-weighted sector return — self-exclusion handled at query time in ml_trainer
        sector_df    = pd.concat(peer_returns.values(), axis=1, sort=False)
        sector_index = sector_df.mean(axis=1).dropna()
        sector_index.index = pd.to_datetime(sector_index.index).tz_localize(None)

        # Upsert each date into MongoDB
        ops = []
        for date, ret in sector_index.items():
            if np.isnan(ret):
                continue
            ops.append(UpdateOne(
                {"sector": sector, "date": date.to_pydatetime()},
                {"$set": {
                    "sector":      sector,
                    "date":        date.to_pydatetime(),
                    "return":      float(ret),
                    "peer_count":  len(peer_returns),
                    "tickers":     list(peer_returns.keys()),
                    "updated_at":  datetime.now(timezone.utc),
                }},
                upsert=True,
            ))

        if ops:
            result = db.sector_indices.bulk_write(ops, ordered=False)
            logger.info(
                "%s: upserted %d dates (peers: %d)",
                sector, result.upserted_count + result.modified_count, len(peer_returns),
            )

    # Create compound index if not already present
    db.sector_indices.create_index([("sector", 1), ("date", 1)], unique=True)
    logger.info("sector_indices compound index ensured.")


if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    try:
        build_sector_indices(client)
    finally:
        client.close()
        logger.info("Sector index build complete.")
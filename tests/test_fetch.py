import requests
import zipfile
import io

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/all-reports",
})

url = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20260731_F_0000.csv.zip"
resp = session.get(url, timeout=15)
print("status:", resp.status_code, "size:", len(resp.content))

with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
    print(zf.namelist())
    with zf.open(zf.namelist()[0]) as f:
        import pandas as pd
        df = pd.read_csv(f)
        print(df.columns.tolist())
        print(df[df["TckrSymb"] == "RELIANCE"].head() if "TckrSymb" in df.columns else df.head())
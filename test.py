from jugaad_data.nse import NSELive
from datetime import date
import pandas as pd

n = NSELive()
data = n.fii_dii_stats()
print(data)
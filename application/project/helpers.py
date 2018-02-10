import os, re
import pandas as pd


def slugify(s):
    return re.sub('[^\w]+', '-', s).lower()


def dataframe_from_file(filename):
    ext = os.path.splitext(filename)[1]
    if ext in ['.xls', '.xlsx']:
        df = pd.read_excel(filename)
        df.columns = [slugify(col) for col in df.columns]
        df["total pre event"] = ""
        df["median pre event"] = ""
        df["mean pre event"] = ""
        df["total during event"] = ""
        df["median during event"] = ""
        df["mean during event"] = ""
        df["total post event"] = ""
        df["median post event"] = ""
        df["mean post event"] = ""
        return df
    # TODO: Create import from CSV
    return None

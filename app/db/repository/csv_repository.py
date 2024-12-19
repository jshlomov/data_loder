import pandas as pd

def create_df_first_csv(file_path):
    csv_to_df = pd.read_csv(file_path, encoding="ISO-8859-1", low_memory=False)

    df = csv_to_df[[
        "eventid", "iyear", "imonth", "iday", "country_txt", "region_txt", "city",
        "latitude", "longitude", "location", "summary", "attacktype1_txt",
        "attacktype2_txt", "attacktype3_txt", "targtype1_txt", "targtype2_txt",
        "targtype3_txt", "gname", "gname2", "gname3", "nperps", "nkill", "nwound"
    ]].copy()

    df['imonth'] = df['imonth'].replace(0, 1)
    df['iday'] = df['iday'].replace(0, 1)

    df["date"] = pd.to_datetime(
        df['iyear'].astype(str) + '-' +
        df['imonth'].astype(str).str.zfill(2) + '-' +
        df['iday'].astype(str).str.zfill(2),
        errors='coerce'
    )

    return df


def create_df_second_csv(file_path):
    df = pd.read_csv(file_path, encoding="ISO-8859-1")

    df["date"] = pd.to_datetime(df["Date"], format="%d-%b-%y", errors='coerce')
    df["date"] = df["date"].apply(lambda x: x.replace(year=x.year - 100) if x.year > 2050 else x)

    df.drop(columns=["Date"], inplace=True)

    df.rename(columns={
        "City": "city",
        "Country": "country_txt",
        "Perpetrator": "gname",
        "Weapon": "attacktype1_txt",
        "Injuries": "nwound",
        "Fatalities": "nkill",
        "Description": "summary"
    }, inplace=True)

    return df


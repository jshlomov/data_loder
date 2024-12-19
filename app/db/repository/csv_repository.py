import csv

import pandas as pd

from app.db.models.attack import AttackModel
from app.db.models.location import LocationModel



# def load_and_merge_csvs(file1_path, file2_path):
#
#     df1 = pd.read_csv(file1_path, encoding="ISO-8859-1")
#     df2 = pd.read_csv(file2_path, encoding="ISO-8859-1")
#
#     df1 = standardize_first_csv(df1)
#
#     df2["date"] = pd.to_datetime(df2["Date"], format="%d-%b-%y").dt.strftime("%Y-%m-%d")
#     df2.drop(columns=["Date"], inplace=True)
#
#     df2.rename(columns={
#
#     })
#
#     # Merge data frames
#     # merged_df = pd.concat([df1, df2], ignore_index=True)
#     # print(df1)
#     print(df2.columns)
#
# load_and_merge_csvs('../data/terror attacks 1000 rows.csv', '../data/RAND_Database_of_Worldwide_Terrorism_Incidents - 5000 rows.csv')





def parse_row(row):
    try:
        location = LocationModel(
            city=row.get("city"),
            country=row.get("country_txt"),
            region=row.get("region_txt"),
            lat=float(row["latitude"]) if row.get("latitude") else None,
            lon=float(row["longitude"]) if row.get("longitude") else None,
            location=row.get("location"),
        )

        attack_types = [row.get(f"attacktype{i}_txt") for i in range(1, 4) if row.get(f"attacktype{i}_txt")]
        target_types = [row.get(f"targtype{i}_txt") for i in range(1, 4) if row.get(f"targtype{i}_txt")]
        gnames = [row.get("gname")]
        gnames += [row.get(f"gname{i}") for i in range(2, 4) if row.get(f"gname{i}")]

        attack = AttackModel(
            event_id=row["eventid"],
            date=(
                f"{int(row['iyear']):04d}-{int(row['imonth']):02d}-{int(row['iday']):02d}"
                if row.get("iyear") and row.get("imonth") and row.get("iday")
                else None
            ),
            location=location,
            attack_types=attack_types,
            target_types=target_types,
            group_names=gnames,
            fatalities=int(row["nkill"]) if row.get("nkill") else None,
            injuries=int(row["nwound"]) if row.get("nwound") else None,
            terrorists_num=int(row["nperps"]) if row.get("nperps") else None,
            summary=row["summary"] if row.get("summary") else None
        )
        return attack
    except ValueError as e:
        print(f"Validation error: {e}")
        return None

def load_csv_to_mongo(file_path, repository):
    with open(file_path, "r", encoding="ISO-8859-1") as file:
        reader = csv.DictReader(file)
        attacks = [parse_row(row) for row in reader]
        return repository.insert_many_attacks(attacks)
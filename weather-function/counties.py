import pandas as pd
import json

df = pd.read_csv(
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_counties_national.zip",
    sep="\t",
    dtype={"GEOID": str},
)

df.columns = df.columns.str.strip()
df = df[["GEOID", "NAME", "USPS", "INTPTLAT", "INTPTLONG"]]

EXCLUDE_STATES = {"AK", "HI", "PR", "GU", "VI", "MP", "AS"}
df = df[~df["USPS"].isin(EXCLUDE_STATES)]

counties = [
    {
        "geoid": row.GEOID,
        "name":  row.NAME,
        "state": row.USPS,
        "lat":   round(row.INTPTLAT, 4),
        "lon":   round(row.INTPTLONG, 4),
    }
    for row in df.itertuples()
]

# Write JSON
with open("us_counties.json", "w") as f:
    json.dump(counties, f, indent=2)
print(f"Wrote us_counties.json ({len(counties)} counties)")

# Write CSV
df_out = pd.DataFrame(counties)
df_out.to_csv("us_counties.csv", index=False)
print(f"Wrote us_counties.csv ({len(counties)} counties)")
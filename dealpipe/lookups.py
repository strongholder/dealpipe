from typing import Any, Dict

from dealpipe import reader

LookupDict = Dict[str, Any]


def build_lookups(file: str) -> LookupDict:
    lookups_df = reader.read(file, sheet=1)
    companies_df = lookups_df[["CompanyId", "CompanyName"]].dropna()

    return dict(
        companies=dict(zip(companies_df["CompanyId"], companies_df["CompanyName"])),
        currencies=lookups_df["Currencies"].dropna().unique().tolist(),
        countries=lookups_df["Countries"].dropna().unique().tolist(),
    )

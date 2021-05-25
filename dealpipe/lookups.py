from typing import Dict, Set, TypedDict

from dealpipe import reader


class LookupDict(TypedDict):
    currencies: Set[str]
    countries: Set[str]
    companies: Dict[int, str]


def build_lookups(file: str) -> LookupDict:
    lookups_df = reader.read(file, sheet=1)
    companies_df = lookups_df[["CompanyId", "CompanyName"]].dropna()

    return LookupDict(
        companies=dict(zip(companies_df["CompanyId"], companies_df["CompanyName"])),
        currencies=set(lookups_df["Currencies"].dropna().unique().tolist()),
        countries=set(lookups_df["Countries"].dropna().unique().tolist()),
    )

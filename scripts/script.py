import csv
import glob
import os.path
from itertools import combinations
from typing import Optional
from pathlib import Path

import networkx as nx
import pandas
import pandas as pd

from scripts.DataTransformer import DataformTransformer
from scripts.bigquery_loader import upload_to_bigquery
from scripts.schema import TRANSFORMED_SCHEMA, RAW_SCHEMA
from scripts.schema import make_raw_schema
# 746,721 782957

practice_code_to_wave = {
    "ASF":	"5a",
    "AS2": "5a",
    "CCSF":	"2a",
    "CSU":	"4",
    "EAST":	"2a",
    "EBF":	"3",
    "EU5": "5c",
    "EUP": "5c",
    "FBU":	"5b",
    "FCW":	"2b",
    "FNW":	"2c",
    "FUP":	"4",
    "FYC":	"5c",
    "BUR3": "5c",
    "BURC": "5c",
    "H01":	"1",
    "H03":	"1",
    "H06":	"1",
    "HNA":	"3",
    "HNF":	"3",
    "JPUF":	"5c",
    "JRO":	"2a",
    "KSF":	"3",
    "MFU":	"5c",
    "MUF": "5b",
    "EU2": "5b",
    "EU11": "5b",
    "EU12": "5b",
    "ORR":	"3",
    "PKU":	"5a",
    "PK2": "5a",
    "PBU": "5c",
    "RAF":	"3",
    "RHF":	"3",
    "RFU": "5a",
    "VUP": "5a",
    "SAF":	"3",
    "SDH":	"1",
    "SFRO":	"2a",
    "U02":	"5c",
    "U03":	"4",
    "U04":	"5b",
    "U07":	"5b",
    "U08":	"5a",
    "U12":	"5d",
    "U15":	"5d",
    "U19":	"5d",
    "U21":	"5c",
    "U22":	"5b",
    "U23":	"5a",
    "U26":	"4",
    "U27":	"4",
    "U29": "5c",
    "U30": "5c",
    "U31": "5c",
    "U47": "5c",
    "U48": "5d",
    "U50": "5d",
    "USF":	"4",
    "WEST":	"2b",
    "WUF":	"4",
    "UVW2": "4",
    "CS3":	"4",
    "CS4":	"4",
    "ZUG":	"4",
}


def set_wave(practiceCode: str):
    if str(practiceCode) in practice_code_to_wave:
        return practice_code_to_wave[str(practiceCode)]
    else:
        return "unknown wave"


def get_priority_wave(practice_codes):
    if isinstance(practice_codes, list):
        waves = [set_wave(pc) for pc in practice_codes]
    else:
        waves = [set_wave(practice_codes)]
    valid = [w for w in waves if w in WAVE_PRIORITY]
    if not valid:
        return "unknown wave"
    return min(valid, key=lambda w: WAVE_PRIORITY[w])
    

# export const DEFAULT_VALUE_MAPPING = {
default_birthdate = "1900-01-01"
default_lastname = "UNKNOWN_LAST_NAME"
default_firstname = "UNKNOWN_FIRST_NAME"

VALID_WAVES = ["5a", "5b", "5c", "5d"]
WAVE_PRIORITY = {"5a": 0, "5b": 1, "5c": 2, "5d": 3}
RAW_PATIENT_DEMOGRAPHIC_FILE_PATH = f"raw_data/oo_candid_patient_extract_20260411_202604110958_00000.csv" # initial: raw_data/oo_candid_patient_extract_20260406_202604060941_00000.csv
SAMPLE_ENTRIES_FILE_NAME = f"sample_file.csv"
RAW_WITH_REASON_FILE = "output/raw_with_reason.csv"
TRANSFORMED_FILE = "output/transformed.csv"
INGEST_DATE = "04_17" # REPLACEME


def test_ingestion():
    transform_ingest_file()
 
    t = make_raw_schema(pandas.read_csv(TRANSFORMED_FILE))
    s = make_raw_schema(pandas.read_csv(RAW_WITH_REASON_FILE))
    upload_to_bigquery(Path(TRANSFORMED_FILE), f"gc_ingest_{INGEST_DATE}", TRANSFORMED_SCHEMA)
    upload_to_bigquery(Path(RAW_WITH_REASON_FILE), f"gc_raw_with_reason_{INGEST_DATE}", s)

def transform_patient_df(patient_demo_df: pandas.DataFrame, data_transformer: DataformTransformer):
    patient_demo_df["birthDate"] = patient_demo_df["birthDate"].fillna(
        pd.to_datetime(default_birthdate)
    )

    patient_demo_df["patientLastModifiedStamp"] = patient_demo_df["patientLastModifiedStamp"].str.split("-").str[0]
    patient_demo_df = data_transformer.transform_dates(
        patient_demo_df,
        [
            "birthDate",
            "primaryCoverageSubscriberDateOfBirth",
            "secondaryCoverageSubscriberDateOfBirth",
            "tertiaryCoverageSubscriberDateOfBirth",
            "primaryCoverageInsurancePeriodStartDate",
            "secondaryCoverageInsurancePeriodStartDate",
            "tertiaryCoverageInsurancePeriodStartDate",
            "primaryCoverageInsurancePeriodEndDate",
            "secondaryCoverageInsurancePeriodEndDate",
            "tertiaryCoverageInsurancePeriodEndDate",
            "last_service_date",
            "patientLastModifiedStamp",
        ],
    )
    patient_demo_df = patient_demo_df.sort_values(
        by="last_service_date", ascending=False
    )
    patient_demo_df["lastName"] = patient_demo_df["lastName"].str.upper()
    patient_demo_df["firstName"] = patient_demo_df["firstName"].str.upper()
    patient_demo_df["middleName"] = patient_demo_df["middleName"].str.upper()
    patient_demo_df["suffix"] = patient_demo_df["suffix"].str.upper()

    patient_demo_df["firstName"] = patient_demo_df["firstName"].fillna(
        default_firstname
    )
    patient_demo_df["lastName"] = patient_demo_df["lastName"].fillna(default_lastname)

    return patient_demo_df

def validate_df(raw_df: pandas.DataFrame, data_transformer: DataformTransformer) -> tuple[pandas.DataFrame, pandas.DataFrame]:
    raw_df["wave"] = raw_df["practiceCode"].apply(set_wave)
    patient_demo_df = raw_df.copy()
    raw_df["failure_reason"] = ""
    
    patient_demo_df = transform_patient_df(patient_demo_df, data_transformer)
    
    not_in_wave = patient_demo_df[~patient_demo_df["wave"].isin(VALID_WAVES)]
    raw_df.loc[not_in_wave.index, "failure_reason"] = "not in wave 5"

    data_transformer.null_failure(patient_demo_df, ["practiceCode"], raw_df, "no practice code")
    data_transformer.null_failure(patient_demo_df, ["mrn"], raw_df, "no mrn")

    is_deceased = patient_demo_df.dropna(subset=["deceased"])
    raw_df.loc[is_deceased.index, "failure_reason"] = "is deceased"

    date_of_birth_in_future = data_transformer.is_in_future(
        patient_demo_df, "birthDate"
    )
    raw_df.loc[date_of_birth_in_future.index, "failure_reason"] = "date of birth in future"
    return patient_demo_df, raw_df

    
def build_provider_fax_lookup():
    provider_files = glob.glob("raw_data/providers/*_ReferringDoctorByPractice.xlsx")
    all_providers = []
    for f in provider_files:
        df = pd.read_excel(f, dtype=str, usecols=["NPI", "Fax Number"])
        all_providers.append(df)
    if not all_providers:
        return {}
    providers = pd.concat(all_providers, ignore_index=True)
    providers = providers.dropna(subset=["NPI", "Fax Number"])
    providers = providers.drop_duplicates(subset=["NPI"])
    return dict(zip(providers["NPI"], providers["Fax Number"]))


def transform_ingest_file():
    with open(
        RAW_PATIENT_DEMOGRAPHIC_FILE_PATH, "r", encoding="utf-8", errors="ignore"
    ) as f:
        total_lines = sum(1 for _ in f)
    print(f"File has {total_lines} lines (including header).")
    load_name = "wave_1_2_3_adt"
    file_suffix = f"{load_name}_{INGEST_DATE}"
    data_transformer = DataformTransformer(file_suffix, load_name)

    raw_df = pd.read_csv(
        RAW_PATIENT_DEMOGRAPHIC_FILE_PATH,
        on_bad_lines="skip",
        dtype=str,
        low_memory=False,
    )
    patient_demo_df, raw_df = validate_df(raw_df, data_transformer)

    failed_indices = raw_df[
        raw_df["failure_reason"].notna() & (raw_df["failure_reason"] != "")
    ].index
    
    patient_demo_df = patient_demo_df.drop(failed_indices, errors='ignore')

    fax_lookup = build_provider_fax_lookup()
    wave5_mask = patient_demo_df["wave"].isin(VALID_WAVES)
    missing_fax = patient_demo_df["referringProviderFax"].isna() | (patient_demo_df["referringProviderFax"] == "")
    has_npi = patient_demo_df["referringProviderNpi"].notna() & (patient_demo_df["referringProviderNpi"] != "")
    fill_mask = wave5_mask & missing_fax & has_npi
    patient_demo_df.loc[fill_mask, "referringProviderFax"] = patient_demo_df.loc[fill_mask, "referringProviderNpi"].map(fax_lookup)
    print(f"Filled {fill_mask.sum() - patient_demo_df.loc[fill_mask, 'referringProviderFax'].isna().sum()} referring provider fax numbers from provider files")

    patient_demo_df = data_transformer.merge(
        patient_demo_df,
        "mrn",
        [
            "practiceCode",
            "accountClass",
            "attendingProviderNpi",
            "attendingProviderFirstName",
            "attendingProviderLastName",
            "attendingProviderMiddleName",
            "attendingProviderPhone",
            "attendingProviderFax",
            "attendingProviderAddressLine1",
            "attendingProviderAddressLine2",
            "attendingProviderCity",
            "attendingProviderState",
            "attendingProviderPostalCode",
            "attendingProviderCountry",
            "referringProviderNpi",
            "referringProviderFirstName",
            "referringProviderLastName",
            "referringProviderMiddleName",
            "referringProviderPhone",
            "referringProviderFax",
            "referringProviderAddressLine1",
            "referringProviderAddressLine2",
            "referringProviderCity",
            "referringProviderState",
            "referringProviderPostalCode",
            "referringProviderCountry",
            "primaryProviderNpi",
            "primaryProviderFirstName",
            "primaryProviderLastName",
            "primaryProviderMiddleName",
            "primaryProviderPhone",
            "primaryProviderFax",
            "primaryProviderAddressLine1",
            "primaryProviderAddressLine2",
            "primaryProviderCity",
            "primaryProviderState",
            "primaryProviderPostalCode",
            "primaryProviderCountry",
        ],
    )
    patient_demo_df["wave"] = patient_demo_df["practiceCode"].apply(get_priority_wave)
    patient_demo_df, _ = group_duplicates_limited(patient_demo_df)

    data_transformer.write_chunks(patient_demo_df, 1)
    patient_demo_df.to_csv(TRANSFORMED_FILE, index=False)
    raw_df.to_csv(RAW_WITH_REASON_FILE, index=False)


def create_alternative_mrn_summary(df_final: pd.DataFrame) -> pd.DataFrame:
    if df_final.empty:
        return pd.DataFrame(
            columns=[
                "Last Seen Window",
                "# of alternative mrns",
                "% of all alternative mrns",
            ]
        )

    df_summary = df_final.copy()

    df_summary["alternative_mrn_last_service_date"] = pd.to_datetime(
        df_summary["alternative_mrn_last_service_date"]
    )

    today = pd.to_datetime("today").normalize() - pd.DateOffset(months=6)

    bins = [
        pd.Timestamp.min,
        today - pd.DateOffset(years=2),
        today - pd.DateOffset(years=1),
        today - pd.DateOffset(months=6),
        today,
    ]

    labels = ["2+ years", "1 year - 2 years", "6 month - 1 year", "0-6 months"]

    df_summary["Last Seen Window"] = pd.cut(
        df_summary["alternative_mrn_last_service_date"],
        bins=bins,
        labels=labels,
        right=True,
        include_lowest=True,
    )

    summary = df_summary.groupby("Last Seen Window", observed=False)[
        "alternative_mrn"
    ].count()

    final_order = ["0-6 months", "6 month - 1 year", "1 year - 2 years", "2+ years"]
    summary = summary.reindex(final_order).reset_index()

    summary = summary.rename(columns={"alternative_mrn": "# of alternative mrns"})

    total_mrns = summary["# of alternative mrns"].sum()

    if total_mrns > 0:
        summary["% of all alternative mrns"] = (
            summary["# of alternative mrns"] / total_mrns
        )

        summary["% of all alternative mrns"] = summary.apply(
            lambda row: f"{row['% of all alternative mrns'] * 100:.0f}%"
            if row["# of alternative mrns"] > 0
            else pd.NA,
            axis=1,
        )
    else:
        summary["% of all alternative mrns"] = pd.NA

    summary["# of alternative mrns"] = summary["# of alternative mrns"].apply(
        lambda x: x if x > 0 else pd.NA
    )

    return summary


def create_last_seen_summary(df_final: pd.DataFrame) -> pd.DataFrame:
    if df_final.empty:
        return pd.DataFrame(
            columns=[
                "Last Seen Window",
                "# of Patients with Duplicates",
                "% of All Duplicate Patients",
            ]
        )

    patients_df = df_final[["true_mrn", "true_mrn_last_service_date"]].drop_duplicates()

    patients_df["true_mrn_last_service_date"] = pd.to_datetime(
        patients_df["true_mrn_last_service_date"]
    )

    today = pd.to_datetime("today").normalize()

    bins = [
        pd.Timestamp.min,
        today - pd.DateOffset(years=2),
        today - pd.DateOffset(years=1),
        today - pd.DateOffset(months=6),
        today,
    ]

    labels = ["2+ years", "1 year - 2 years", "6 month - 1 year", "0-6 months"]

    patients_df["Last Seen Window"] = pd.cut(
        patients_df["true_mrn_last_service_date"],
        bins=bins,
        labels=labels,
        right=True,
        include_lowest=True,
    )

    summary = patients_df.groupby("Last Seen Window", observed=False)[
        "true_mrn"
    ].count()

    final_order = ["0-6 months", "6 month - 1 year", "1 year - 2 years", "2+ years"]
    summary = summary.reindex(final_order).reset_index()

    summary = summary.rename(columns={"true_mrn": "# of Patients with Duplicates"})

    total_patients = summary["# of Patients with Duplicates"].sum()

    if total_patients > 0:
        summary["% of All Duplicate Patients"] = (
            summary["# of Patients with Duplicates"] / total_patients
        )

        summary["% of All Duplicate Patients"] = summary.apply(
            lambda row: f"{row['% of All Duplicate Patients'] * 100:.0f}%"
            if row["# of Patients with Duplicates"] > 0
            else pd.NA,
            axis=1,
        )
    else:
        summary["% of All Duplicate Patients"] = pd.NA

    summary["# of Patients with Duplicates"] = summary[
        "# of Patients with Duplicates"
    ].apply(lambda x: x if x > 0 else pd.NA)

    return summary


def group_duplicates_limited(df: pd.DataFrame):
    df["last_service_date"] = pd.to_datetime(df["last_service_date"])
    normalized_ssn = (
        df["socialSecurityNumber"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    repeated_char_pattern = r"^(.)\1+$"
    is_bad_ssn = (
        normalized_ssn.isin(
            [
                "0",
                "1111",
                "111111111",
                "000001111",
                "777777777",
                "555555555",
                "999999999",
                "7777",
                "123456789",
                "000007777",
                "1",
                "5555",
                "000004114",
                "00004114",
                "0004114",
                "004114",
                "0000004114",
            ]
        )
        | normalized_ssn.isna()
        | normalized_ssn.astype(str).str.match(repeated_char_pattern)
    )

    ssn_df = group_by(df[~is_bad_ssn], ["socialSecurityNumber"], "2023-11-01")
    name_dob = ["firstName", "lastName", "birthDate"]
    name_dob_df = group_by(df, name_dob, "2023-11-01")
    address_dob = [
        "birthDate",
        "primaryAddressLine1",
        "primaryAddressCity",
        "primaryAddressState",
    ]
    address_dob_df = group_by(df, address_dob, "2023-11-01")

    combined_duplicates = pd.concat([ssn_df, name_dob_df, address_dob_df])
    combined_duplicates = resolve_overlapping_duplicates(combined_duplicates)
    combined_duplicates["total_duplicates"] = combined_duplicates["all_mrns"].apply(
        lambda x: len(x) - 1
    )

    to_map = combined_duplicates.copy()
    to_map["all_mrns2"] = to_map["all_mrns"]
    mrn_mapper = to_map[
        ["all_mrns", "true_mrn", "last_service_date", "all_mrns2"]
    ].explode("all_mrns")
    mrn_mapper = mrn_mapper.drop_duplicates(subset=["all_mrns"])

    mrn_to_all_mrns_map = mrn_mapper.set_index("all_mrns")["all_mrns2"]

    df["all_mrns"] = df["mrn"].map(mrn_to_all_mrns_map)

    df_duplicates = df

    lead_columns = ["mrn", "last_service_date", "wave", "all_mrns"]

    all_columns = df_duplicates.columns.tolist()

    other_columns = [
        col for col in all_columns if col not in lead_columns and "Unnamed:" not in col
    ]

    final_columns = lead_columns + other_columns

    df_final = df_duplicates[final_columns]

    df_final = df_final.reset_index(drop=True)

    duplicate_report = combined_duplicates.copy()
    # duplicate_report = duplicate_report[duplicate_report['total_duplicates'] != 0]

    def alt_mrns(row):
        return [mrn for mrn in row["all_mrns"] if mrn != row["true_mrn"]]

    if len(duplicate_report) > 0:
        duplicate_report["alternative_mrns"] = duplicate_report.apply(alt_mrns, axis=1)
    else:
        duplicate_report["alternative_mrns"] = []
    report_columns = ["true_mrn", "alternative_mrns", "total_duplicates"]
    duplicate_report = duplicate_report[report_columns]
    duplicate_report = duplicate_report.drop_duplicates(subset=["true_mrn"])

    return df_final, duplicate_report


def resolve_overlapping_duplicates(combined_duplicates: pd.DataFrame) -> pd.DataFrame:
    if combined_duplicates.empty:
        return combined_duplicates

    df = combined_duplicates.reset_index(drop=True).copy()

    df_exploded = df.reset_index().explode("all_mrns")

    G = nx.Graph()
    G.add_nodes_from(df.index)

    # Note: Handle potential NaNs in 'all_mrns' if they can exist
    df_exploded_no_na = df_exploded.dropna(subset=["all_mrns"])
    mrn_to_indices = df_exploded_no_na.groupby("all_mrns")["index"].agg(list)

    for indices in mrn_to_indices:
        if len(indices) > 1:
            for u, v in combinations(indices, 2):
                G.add_edge(u, v)

    connected_groups = list(nx.connected_components(G))

    aggregated_rows = []
    for group_indices in connected_groups:
        component_df = df.loc[list(group_indices)]

        all_mrns_set = set().union(*component_df["all_mrns"])
        new_all_mrns = list(all_mrns_set)
        all_practice = set().union(*component_df["all_mrns"])
        new_all_mrns = list(all_mrns_set)

        # This line is safe; max() on all-NaT returns NaT
        most_recent_date = component_df["last_service_date"].max()

        # --- FIX ---
        # Check if the max date is NaT. This happens if all dates in the
        # component_df were NaT.
        if pd.isna(most_recent_date):
            # If so, idxmax() will fail. We must pick an arbitrary 'true_mrn'
            # as a fallback. We'll pick the 'true_mrn' from the first row.
            new_true_mrn = component_df.iloc[0]["true_mrn"]
        else:
            # If we have a valid max date, it is now safe to use idxmax()
            best_row_index = component_df["last_service_date"].idxmax()
            new_true_mrn = component_df.loc[best_row_index, "true_mrn"]
        # --- END FIX ---

        aggregated_rows.append(
            {
                "all_mrns": new_all_mrns,
                "last_service_date": most_recent_date,
                "true_mrn": new_true_mrn,
            }
        )

    cleaned_df = pd.DataFrame(aggregated_rows)

    return cleaned_df


def group_by(df: pd.DataFrame, columns: list[str], cut_off: Optional[str]):
    df_cleaned = df.dropna(subset=columns)
    if cut_off:
        df_cleaned = df_cleaned[df_cleaned["last_service_date"] > cut_off]

    if df_cleaned.empty:
        # Handle empty dataframe case
        return pd.DataFrame(columns=["true_mrn", "last_service_date", "all_mrns"])

    # --- This part is fine ---
    main_agg = df_cleaned.groupby(columns).agg(
        all_mrns=("mrn", list), all_practice_codes=("practiceCode", list), last_service_date=("last_service_date", "max")
    )

    df_sorted = df_cleaned.sort_values(by="last_service_date", ascending=False)

    df_true_mrn = df_sorted.drop_duplicates(subset=columns, keep="first")

    true_mrn_series = df_true_mrn.set_index(columns)["mrn"]

    main_agg = main_agg.join(true_mrn_series.rename("true_mrn")).reset_index()

    main_agg = main_agg[["true_mrn", "last_service_date", "all_mrns", "all_practice_codes"]]
    return main_agg

    # ssn_group = df.copy()
    # ssn_group = ssn_group.groupby("socialSecurityNumber").agg(
    #     all_mrns=('mrn', 'list'),
    #     last_service_mrn_date=('last_service_date', 'max')
    # )
    # address_dob = ["birthDate", "primaryAddressLine1", "primaryAddressCity", "primaryAddressState"]
    # name_dob = ["firstName", "lastName", "birthDate"]
    #
    # duplicate_ssn = df[df.duplicated(subset=["socialSecurityNumber"], keep=False)]
    # na_ssn_mask = duplicate_ssn['socialSecurityNumber'].isna()
    # is_default_mask = duplicate_ssn['socialSecurityNumber'].apply(is_default_ssn)
    # duplicate_ssn = duplicate_ssn[~(na_ssn_mask | is_default_mask)]
    # duplicate_ssn["category"] = "ssn"
    # duplicate_ssn = duplicate_ssn.sort_values(by="socialSecurityNumber")
    # duplicate_ssn.head(100).to_csv("sample_duplicates.csv")
    #
    # duplicate_address_dob = df[df.duplicated(subset=address_dob, keep=False)]
    # duplicate_address_dob["category"] = "address dob"
    # duplicate_name_dob = df[df.duplicated(subset=name_dob, keep=False)]
    # duplicate_name_dob["category"] = "name dob"
    #
    # combined_duplicates = pd.concat([duplicate_ssn, duplicate_address_dob, duplicate_name_dob])


def group_duplicate(
    original_df: pd.DataFrame, category: str, keys: list[str]
) -> pd.DataFrame:
    valid_keys = [key for key in keys if key in original_df.columns]
    if not valid_keys or len(valid_keys) != len(keys):
        print(f"Warning: Skipping category '{category}' due to missing keys.")
        return pd.DataFrame(columns=original_df.columns)

    df = original_df.copy()

    df = df.dropna(subset=valid_keys)
    if df.empty:
        return df

    df["group_min_date"] = df.groupby(valid_keys)["last_service_date"].transform("min")
    df["group_max_date"] = df.groupby(valid_keys)["last_service_date"].transform("max")

    df["group_min_date"] = pd.to_datetime(df["group_min_date"], errors="coerce")
    df["group_max_date"] = pd.to_datetime(df["group_max_date"], errors="coerce")

    df["date_span"] = df["group_max_date"] - df["group_min_date"]

    df["group_count"] = df.groupby(valid_keys)[valid_keys[0]].transform("count")

    df["duplicate_type"] = category
    df = df[df["group_count"] > 1].copy()

    return df

def write_duplicates(df: pd.DataFrame, suffix: str):
    data_transformer = DataformTransformer(f"duplicates_{suffix}", "duplicate_check")
    name_dob_columns = ["firstName", "lastName", "birthDate"]

    duplicate_name_dob = data_transformer.write_duplicate_df(
        df, "name_dob", name_dob_columns, False
    )
    duplicate_ssn = data_transformer.write_duplicate_df(
        df, "ssn", ["socialSecurityNumber"], True
    )
    duplicate_address_and_dob = data_transformer.write_duplicate_df(
        df,
        "dob address",
        [
            "birthDate",
            "primaryAddressLine1",
            "primaryAddressCity",
            "primaryAddressState",
        ],
        exclude_nulls=True,
    )

    df = pd.concat(
        [
            duplicate_name_dob.assign(duplicate_category="name_dob"),
            duplicate_ssn.assign(duplicate_category="ssn"),
            duplicate_address_and_dob.assign(duplicate_category="address_and_dob"),
        ]
    )

    today = pd.to_datetime("today").normalize()

    date_6_months_ago = today - pd.DateOffset(months=6)
    date_1_year_ago = today - pd.DateOffset(years=1)
    date_2_years_ago = today - pd.DateOffset(years=2)

    df["last_service_date"] = pd.to_datetime(df["last_service_date"], errors="coerce")
    df[df["last_service_date"] > date_6_months_ago].to_csv(
        f"duplicates/{suffix}_duplicates_6_months.csv"
    )
    df[df["last_service_date"] > date_1_year_ago].to_csv(
        f"duplicates/{suffix}_duplicates_1_year.csv"
    )
    df[df["last_service_date"] > date_2_years_ago].to_csv(
        f"duplicates/{suffix}_duplicates_2_year.csv"
    )
    df[df["last_service_date"] <= date_2_years_ago].to_csv(
        f"duplicates/{suffix}duplicates_more_2_year.csv"
    )
    pass


def read(file, columns: list[str]):
    df = pandas.read_csv(file)
    print("length ", len(df))
    print("nulls ", df[columns].isnull().sum())
    print("value count", df[columns].value_counts())
    print("value count ascending", df[columns].value_counts(ascending=True))

from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from pandas import DataFrame
import math


class DataformTransformer:
    """
    Has a collection of methods that given a DataFrame return a NEW dataframe with some transformation applied to it. Uses file suffix and load name for writing files properly named.
    """

    def __init__(self, file_suffix: str, load_name: str):
        self.file_suffix = file_suffix
        self.load_name = load_name

    def write_chunks(self, df: DataFrame, chunks: int) -> None:
        dfs_chunked = np.array_split(df, chunks)
        for i, df_chunk in enumerate(dfs_chunked):
            df_chunk.to_csv(f"output/gc_patient_demo_{self.file_suffix}_part{i + 1}.csv", index=False, na_rep="")

    def remove_bad_rows(self, original_df: DataFrame, invalid_rows_df: DataFrame, on_str: Optional[str]) -> DataFrame:
        df = original_df[~original_df['mrn'].isin(invalid_rows_df['mrn'])]
        if on_str:
            print(f"Removing {len(invalid_rows_df)} rows because they are ", on_str)
        return df

    def pick_duplicate_with_most_data(self, original_df: DataFrame, category: str, pick_one_column: str, exclude_nulls: bool) -> DataFrame:
        duplicate_df = self.write_duplicate_df(original_df, category, [pick_one_column], exclude_nulls)
        duplicate_values = set(duplicate_df[pick_one_column].to_list())
        
        picked = []
        for dup in duplicate_values:
            duplicate_rows = duplicate_df[duplicate_df[pick_one_column] == dup]
            picked.append(self._most_data(duplicate_rows))
            
        no_duplicates_df = self.remove_bad_rows(original_df, duplicate_df, None)
        return pd.concat([no_duplicates_df, *picked], ignore_index=True)
    
    def _most_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if dataframe.empty:
            return None

        temp_df = dataframe.replace('', np.nan)
        filled_counts = temp_df.count(axis=1)
        max_index = filled_counts.idxmax()

        return dataframe.loc[[max_index]]
    
    def transform_dates(self, original_df: DataFrame, date_columns: list[str]):
        for col in date_columns:
            original_df[col] = pd.to_datetime(original_df[col], format='mixed', errors="coerce").dt.strftime('%Y-%m-%d')
        return original_df
    
    def is_in_future(self, original_df: DataFrame, column: str):
        new_df = pd.DataFrame(original_df)
        new_df[column] = pd.to_datetime(
            new_df[column],
            errors='coerce'
        )
        now = datetime.now()
        return new_df[new_df[column] >= now]

    def null_failure(self, original_df: DataFrame, nullable_columns: list[str], raw_df: DataFrame, failure_reason: str):
        nulls = original_df[original_df[nullable_columns].isnull().any(axis=1)]
        raw_df.loc[nulls.index, "failure_reason"] = failure_reason
        return nulls

    def write_nulls_df(self, original_df: DataFrame, category: str, nullable_columns: list[str]) -> DataFrame:
        null_df = original_df[original_df[nullable_columns].isnull().any(axis=1)]
        # TODO if default value then set it 

        print(f"Nulls in {category} has {len(null_df)} instances")
        null_df.to_csv(f"output/gc_nulls_{category}_{self.file_suffix}.csv")
        return null_df

    def write_duplicate_df(self, original_df: DataFrame, category: str, unique_columns: list[str], exclude_nulls: bool) -> DataFrame:
        duplicate_df = original_df[original_df.duplicated(subset=unique_columns, keep=False)]
        if exclude_nulls:
            duplicate_df = duplicate_df.dropna(subset=unique_columns)

        print(f"Duplicate {category} has {len(duplicate_df)} instances")
        duplicate_df = duplicate_df.sort_values(by=unique_columns)
        duplicate_df.to_csv(f"output/gc_duplicates_{category}_{self.file_suffix}.csv")
        return duplicate_df

    def merge(self, original_df: DataFrame, merge_on: str, combine_columns: list[str]):
        
        def agg_combine(series: pd.Series):
            if len(series) == 1:
                return series.iloc[0]
            else:
                return list([s if (s and not str(s).lower() == "nan") else "NONE" for s in series])

        # Get all columns *except* the one we are merging on
        all_cols = original_df.columns.drop(merge_on)

        # Build the aggregation dictionary
        agg_dict = {}
        for col in all_cols:
            if col in combine_columns:
                # Use our custom logic for specified columns
                agg_dict[col] = agg_combine
            else:
                # Use 'first' for all other columns (like 'name', 'age')
                agg_dict[col] = 'first'

        # Group by the merge_on column, apply the aggregations, and reset the index
        merged_df = original_df.groupby(merge_on).agg(agg_dict).reset_index()

        return merged_df
    
    def sort_by_data(self, original_df: DataFrame):
        return (
            original_df.assign(
                filled_count = lambda x: x.fillna(0).map(bool).sum(axis=1)
            )
            .sort_values(by='filled_count', ascending=False)
            .drop(columns='filled_count')
        )


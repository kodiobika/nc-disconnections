import pandas as pd
import json
from pandas.api.types import is_string_dtype

with open('config.json') as f:
    config = json.load(f)

def preprocess_docket_data(df):
    # Filter to include only Duke and Dominion electric utilities
    electric_utility_mask = df["utility_type"] == "Electric"
    duke_mask = df["utility_name"].str.contains("Duke")
    dominion_mask = df["utility_name"].str.contains("Dominion")
    df = (
        df.loc[electric_utility_mask & (duke_mask | dominion_mask)]
        .copy()
        .reset_index(drop=True)
        .rename(columns={
            "accounts_disconnected": "disconnections"
        })
    )

    try:
        df["reporting_period"] = pd.to_datetime(
            df.reporting_period, format="%m/%d/%y"
        )
    except:
        pass
    df["reporting_period"] = df.reporting_period.dt.strftime('%b %Y')

    # Convert numeric columns to actual numbers
    non_numeric_columns = ["utility_type", "utility_name", "reporting_period"]
    for col in df.drop(columns=non_numeric_columns).columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Standardize utility names
    df.loc[(
        df.utility_name.str.contains("Duke Energy Carolinas"), "utility_name"
    )] = "Duke Energy Carolinas"
    df.loc[(
        df.utility_name.str.contains("Duke Energy Progress"), "utility_name"
    )] = "Duke Energy Progress"
    df.loc[(
        df.utility_name.str.contains("Dominion"), "utility_name"
    )] = "Dominion Energy"

    # Add disconnection rates
    df["disconnection_rate"] = df["disconnections"] / df["accounts"]
    df[">24h_disconnection_rate"] = (
        1 - (df["accounts_disconnected_<24h"] / df["disconnections"])
    )

    return df

def preprocess_historical_data(df):
    month_abbr_map = {
        "January": "Jan",
        "February": "Feb",
        "March": "Mar",
        "April": "Apr",
        "May": "May",
        "June": "Jun",
        "July": "Jul",
        "August": "Aug",
        "September": "Sep",
        "October": "Oct",
        "November": "Nov",
        "December": "Dec"
    }
    # Filter to include only pre-August 2022 disconnections
    df["reporting_period"] = (
        df.month.map(month_abbr_map) + ' ' + df.year.astype(str)
    )
    df = (
        df.loc[pd.to_datetime(df['reporting_period'], format='%b %Y') < '2022-08-01']
        .copy()
    )
    df["disconnection_rate"] /= 100

    return df


def add_pre_2021_dominion_account_data(df_in):
    dom_accs_all = pd.DataFrame()

    for year in range(2017, 2021):
        df = pd.read_excel(f'data/dominion/retail_sales_{year}.xlsx')
        dom_accs_year = (
            df.loc[(
                (df["Unnamed: 3"] == "Virginia Electric & Power Co")
                & (df["Unnamed: 4"] == "NC")
            )]
            [["UTILITY CHARATERISTICS", "Unnamed: 1", "Unnamed: 9"]]
            .rename(columns={
                "UTILITY CHARATERISTICS": "year",
                "Unnamed: 1": "month",
                "Unnamed: 9": "residential_accounts"
            })
            .assign(day=1, utility_name="Dominion Energy")
        )
        dom_accs_year["timestamp"] = pd.to_datetime(dom_accs_year[['month', 'year','day']])
        dom_accs_year = dom_accs_year[["utility_name", "timestamp", "residential_accounts"]]
        dom_accs_all = pd.concat([dom_accs_all, dom_accs_year]).reset_index(drop=True)

    for year in range(2013, 2017):
        df = pd.read_excel(f'data/dominion/f826{year}.xls')
        try:
            dom_accs_year = (
                df.loc[(
                    (df["Unnamed: 3"] == "Virginia Electric & Power Co")
                    & (df["Unnamed: 4"] == "NC")
                )]
                [["UTILITY CHARACTERISTICS", "Unnamed: 1", "Unnamed: 8"]]
                .rename(columns={
                    "UTILITY CHARACTERISTICS": "year",
                    "Unnamed: 1": "month",
                    "Unnamed: 8": "residential_accounts"
                })
                .assign(day=1, utility_name="Dominion Energy")
            )
            dom_accs_year["timestamp"] = pd.to_datetime(dom_accs_year[['month', 'year','day']])
            dom_accs_year = dom_accs_year[["utility_name", "timestamp", "residential_accounts"]]
            dom_accs_all = pd.concat([dom_accs_all, dom_accs_year]).reset_index(drop=True)
        except:
            dom_accs_year = (
                df.loc[(df["Unnamed: 3"] == "Virginia Electric & Power Co") & (df["Unnamed: 4"] == "NC")]
                [["UTILITY CHARATERISTICS", "Unnamed: 1", "Unnamed: 9"]]
                .rename(columns={
                    "UTILITY CHARATERISTICS": "year",
                    "Unnamed: 1": "month",
                    "Unnamed: 9": "residential_accounts"
                })
                .assign(day=1, utility_name="Dominion Energy")
            )
            dom_accs_year["timestamp"] = pd.to_datetime(dom_accs_year[['month', 'year','day']])
            dom_accs_year = dom_accs_year[["utility_name", "timestamp", "residential_accounts"]]
            dom_accs_all = pd.concat([dom_accs_all, dom_accs_year]).reset_index(drop=True)

    for year in range(2000, 2013):
        df = pd.read_excel(f'data/dominion/f826{year}.xls')
        try:
            dom_accs_year = (
                df.loc[(
                    (df.UTILNAME == "Virginia Electric & Power Co")
                    & (df.STATE_CODE == 'NC')
                )]
                [['MONTH', 'YEAR', 'RES_CONS ']]
                .rename(columns={
                    "YEAR": "year",
                    "MONTH": "month",
                    "RES_CONS ": "residential_accounts"
                })
                .assign(day=1, utility_name="Dominion Energy")
            )
        except:
            try:
                dom_accs_year = (
                    df.loc[(
                        (df.UTILNAME == "Virginia Electric & Power Co")
                        & (df.STATE == 'NC')
                    )]
                    [['MONTH', 'YEAR', 'RES_CONS ']]
                    .rename(columns={
                        "YEAR": "year",
                        "MONTH": "month",
                        "RES_CONS ": "residential_accounts"
                    })
                    .assign(day=1, utility_name="Dominion Energy")
                )
            except:
                dom_accs_year = (
                    df.loc[(
                        (df.UTILNAME == "Virginia Electric & Power Co")
                        & (df.STATE_CODE == 'NC')
                    )]
                    [['MONTH', 'YEAR', 'RESIDENTIAL CUSTOMERS']]
                    .rename(columns={
                        "YEAR": "year",
                        "MONTH": "month",
                        "RESIDENTIAL CUSTOMERS": "residential_accounts"
                    })
                    .assign(day=1, utility_name="Dominion Energy")
                )
        if is_string_dtype(dom_accs_year['year']):
            dom_accs_year['year'] = dom_accs_year['year'].str[:4].astype(int)
        dom_accs_year["timestamp"] = pd.to_datetime(dom_accs_year[['month', 'year','day']])
        dom_accs_year = dom_accs_year[["utility_name", "timestamp", "residential_accounts"]]
        dom_accs_all = pd.concat([dom_accs_all, dom_accs_year]).reset_index(drop=True)

    df_out = df_in = (
        df_in.merge(dom_accs_all, on=["utility_name", "timestamp"], how="left")
    )
    df_out["disconnection_rate"] = (
        df_out["disconnection_rate"].fillna(df_out['disconnections'] / df_out['residential_accounts'])
    )
    df_out['residential_accounts'] = (
        df_out['disconnections'] / df_out['disconnection_rate']
    )
    return df_out

def create_disconnection_dataset():
    df_0822_1223 = (
        pd.read_excel("data/dc_23.xlsx")
        .rename(columns=config["aug22_dec23_column_map"])
        .replace(',','', regex=True)
    )
    df_0822_1223 = preprocess_docket_data(df_0822_1223)

    df_0222_0722 = (
        pd.read_csv("data/disconnects_22.csv")
        .rename(columns=config["feb22_jul22_column_map"])
        .iloc[1:]
    )
    df_0222_0722 = preprocess_docket_data(df_0222_0722)

    df_hist = pd.read_csv("data/pre_22_disconnections.csv")
    df_hist = preprocess_historical_data(df_hist)

    all_disconnections = (
        pd.concat([df_0822_1223, df_0222_0722, df_hist])
        [["reporting_period", "utility_name", "disconnections", "disconnection_rate"]]
        .assign(timestamp=lambda x: pd.to_datetime(x.reporting_period, format='%b %Y'))
        .drop_duplicates(subset=["reporting_period", "utility_name"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    all_disconnections = add_pre_2021_dominion_account_data(all_disconnections)

    (
        all_disconnections[[
            "timestamp",
            "reporting_period",
            "utility_name",
            "residential_accounts",
            "disconnections",
            "disconnection_rate"
        ]]
        .sort_values("timestamp")
        .to_csv("data/nc_electric_disconnections_1996_2023.csv", index=False)
    )
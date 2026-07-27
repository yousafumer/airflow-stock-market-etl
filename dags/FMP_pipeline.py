from airflow.decorators import dag, task
from airflow.models import Variable


from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas


from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup


@dag(
    dag_id="sp500_company_profile_etl",
    start_date=datetime(2026, 7, 26),
    schedule="30 8  * * 6",          # Change to "*/5 * * * *" later if needed
    catchup=False,
    tags=["sp500", "api"]
)

def sp500_fmp_pipeline():

    @task
    def scrape_symbols():

        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/138.0.0.0 Safari/537.36"
            )
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", id="constituents")

        headers = [
            th.get_text(strip=True)
            for th in table.find("tr").find_all("th")
        ]

        data = []

        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            data.append([col.get_text(strip=True) for col in cols])

        df = pd.DataFrame(data, columns=headers)

        symbol_list = df["Symbol"].tolist()[:50]

        print(symbol_list)

        return symbol_list


    @task
    def fetch_company_profile(symbol_list):

        api_key = Variable.get("FMP_API_KEY")

        all_data = []

        for symbol in symbol_list:

            url = (
                f"https://financialmodelingprep.com/stable/profile"
                f"?symbol={symbol}&apikey={api_key}"
            )

            response = requests.get(url)
            response.raise_for_status()

            company = response.json()

            all_data.extend(company)

            print(company)

        return all_data


    @task
    def load_to_snowflake(company_data):

        hook = SnowflakeHook(
    snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()

        df = pd.DataFrame(company_data)

        print(df.columns.tolist())
        print(df.head())

        success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="COMPANY_PROFILE",
        quote_identifiers=False
        )

        print(success, nchunks, nrows)
        print(f"Loaded {nrows} rows to Snowflake")

        conn.close()

    

    symbols = scrape_symbols()

    company_data = fetch_company_profile(symbols)

    load_to_snowflake(company_data)


dag = sp500_fmp_pipeline()
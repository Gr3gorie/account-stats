import gspread
from google.oauth2.service_account import Credentials
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime
import psycopg2
import os

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SERVICE_ACCOUNT_FILE = ''


class Act(BaseModel):
    project_name: Optional[str]
    current_debt: Optional[float]
    act_sum: Optional[float]
    act_number: Optional[str]
    contractor: Optional[str]
    inn: Optional[str]
    contract_number: Optional[str]
    created_date: Optional[date]
    signed_date: Optional[date]


creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPE)
client = gspread.authorize(creds)


def parse_currency(value: str) -> Optional[float]:
    if value:
        try:
            return float(value.replace('₽', '').replace(',', '').strip())
        except ValueError:
            print(f"Warning: Could not parse currency value '{value}'.")
    return None


def parse_date(value: str) -> Optional[date]:
    if value:
        try:
            return datetime.strptime(value, "%d-%b-%Y").date()
        except ValueError:
            print(f"Warning: Could not parse date '{value}'.")
    return None


def get_acts_from_sheet(sheet_name: str) -> List[Act]:
    sheet = client.open("Трекер документов").worksheet(sheet_name)

    data = sheet.get_all_values()[3:]
    processed_data = []

    current_project_name = None
    current_contractor = None
    current_inn = None

    for row in data:
        if not any([row[0], row[8], row[11], row[12], row[16], row[17]]):
            continue

        if row[16]:
            if row[16] != current_contractor and not row[0]:
                current_project_name = None
                current_inn = None

            current_contractor = row[16]

        if row[0]:
            current_project_name = row[0]

        if row[17]:
            current_inn = row[17]

        processed_row = Act(
            project_name=current_project_name,
            current_debt=parse_currency(row[8]),
            act_sum=parse_currency(row[11]),
            act_number=row[12],
            contractor=current_contractor,
            inn=current_inn,
            contract_number=row[4],
            created_date=parse_date(row[13]),
            signed_date=parse_date(row[14])
        )

        processed_data.append(processed_row)

    return processed_data


def create_and_insert_into_postgres(conn, table_name: str, acts: List[Act]):
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            project_name VARCHAR(255),
            current_debt FLOAT,
            act_sum FLOAT,
            act_number VARCHAR(255),
            contractor VARCHAR(255),
            inn VARCHAR(50),
            contract_number VARCHAR(255),
            created_date DATE,
            signed_date DATE
        );
    """)

    insert_query = f"""
        INSERT INTO {table_name} (
            project_name, current_debt, act_sum, act_number, contractor, inn,
            contract_number, created_date, signed_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for act in acts:
        data_tuple = (
            act.project_name,
            act.current_debt,
            act.act_sum,
            act.act_number,
            act.contractor,
            act.inn,
            act.contract_number,
            act.created_date,
            act.signed_date
        )

        cur.execute(insert_query, data_tuple)

    conn.commit()
    cur.close()


def main():
    print("Connecting to Postgres...")
    conn = psycopg2.connect(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        database=os.environ["POSTGRES_DATABASE"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"]
    )

    print("Getting acts...")
    acts_ip = get_acts_from_sheet("ИП входящие")
    acts_ooo = get_acts_from_sheet("ООО входящие")

    print("Running migrations and inserting data...")
    create_and_insert_into_postgres(conn, "ip_acts", acts_ip)
    create_and_insert_into_postgres(conn, "ooo_acts", acts_ooo)

    conn.close()
    print("Data successfully inserted.")


if __name__ == "__main__":
    main()

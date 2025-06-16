import pandas as pd
import sqlite3


def create_database(db_name: str) -> None:
    """
    Cria o datawarehouse.

    Args:
        db_name (str): Nome do banco de dados.
    """
    conn = sqlite3.connect(db_name)
    conn.close()
    print(f"Database {db_name} created")


def create_table(db_name: str, table_name: str) -> None:
    """
    Cria a tabela no datawarehouse.

    Args:
        db_name (str): Nome do banco de dados.
        table_name (str): Nome da tabela.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            year INTEGER,
            state TEXT,
            month TEXT,
            month_number INTEGER,
            month_year TEXT,
            number FLOAT,
            UNIQUE(year, state, month)
        )
    """)
    conn.commit()
    conn.close()
    print(f"Table {table_name} created in {db_name}")


def insert_data(df: pd.DataFrame, db_name: str, table_name: str) -> None:
    """
    Insere os dados no datawarehouse.
    Args:
        df (pd.DataFrame): DataFrame do pandas com os dados.
        db_name (str): Nome do banco de dados.
        table_name (str): Nome da tabela.
    """
    conn = sqlite3.connect(db_name)
    sql: str = f"""
        INSERT OR REPLACE INTO {table_name} (year, state, month, month_number, month_year, number)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    for _, row in df.iterrows():
        conn.execute(
            sql, (row["year"], row["state"], row["month"], row["month_number"], row["month_year"], row["number"])
        )
    conn.commit()
    print(f"Inserted {len(df)} rows into {table_name}")
    conn.close()
    print(f"Data inserted into {table_name} in {db_name}")


if __name__ == "__main__":
    db_name = "databases/datawarehouse.db"
    table_name = "fires"

    create_database(db_name)
    create_table(db_name, table_name)
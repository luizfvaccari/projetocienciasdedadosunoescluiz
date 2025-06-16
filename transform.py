import pandas as pd
import sqlite3


def read_sqlite(db_name: str, table_name: str) -> pd.DataFrame:
    """
    Lê os dados de uma tabela em um banco de dados SQLite e retorna um DataFrame do pandas.

    Args:
        db_name (str): Nome do banco de dados.
        table_name (str): Nome da tabela.

    Returns:
        pd.DataFrame: DataFrame do pandas com os dados lidos.
    """
    conn = sqlite3.connect(db_name)

    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    conn.close()

    return df


def analyze_data(df: pd.DataFrame) -> None:
    """
    Análise inicial dos dados para entender a estrutura e o conteúdo.

    Args:
        df (pd.DataFrame): DataFrame do pandas com os dados.
    """
    print("Data Analysis:")
    print("states:", df["state"].unique())
    print("years:", df["year"].unique())
    print("months:", df["month"].unique())
    # Verifica se existem datas diferentes para cada combinação de state, year e month
    duplicates = df.groupby(["state", "year", "month"])["date"].nunique()
    duplicates = duplicates[duplicates > 1]
    if not duplicates.empty:
        print("Duplicated dates for each state, year and month:")
        print(duplicates)


def transform_state_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma o nome dos estados conforme necessário.

    Args:
        df (pd.DataFrame): DataFrame do pandas com os dados.

    Returns:
        pd.DataFrame: DataFrame do pandas com os nomes dos estados transformados.
    """
    df["state"] = df["state"].replace("Rio", "Rio de Janeiro")
    df["state"] = df["state"].replace("Piau", "Piaui")

    return df


def agg_per_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregação dos dados por estado, ano e mês.

    Args:
        df (pd.DataFrame): DataFrame do pandas com os dados.

    Returns:
        pd.DataFrame: DataFrame do pandas com os dados agregados.
    """
    agg_df = df.groupby(["state", "year", "month"]).agg({"number": "sum"}).reset_index()

    return agg_df


def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas desnecessárias do DataFrame.

    Args:
        df (pd.DataFrame): DataFrame do pandas com os dados.

    Returns:
        pd.DataFrame: DataFrame do pandas com as colunas removidas.
    """
    print(df.head(5))
    df = df.drop(columns=["date"])

    return df


def add_month_number(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona o número do mês ao DataFrame.

    Args:
        df (pd.DataFrame): DataFrame do pandas com os dados.

    Returns:
        pd.DataFrame: DataFrame do pandas com o número do mês adicionado.
    """
    month_mapping = {
        "Janeiro": 1,
        "Fevereiro": 2,
        "Marco": 3,
        "Abril": 4,
        "Maio": 5,
        "Junho": 6,
        "Julho": 7,
        "Agosto": 8,
        "Setembro": 9,
        "Outubro": 10,
        "Novembro": 11,
        "Dezembro": 12,
    }

    df["month_number"] = df["month"].map(month_mapping)

    return df


def add_month_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona uma coluna de mês e ano ao DataFrame.

    Args:
        df (pd.DataFrame): DataFrame do pandas com os dados.

    Returns:
        pd.DataFrame: DataFrame do pandas com a nova coluna adicionada.
    """
    df["month_year"] = df["month_number"].astype(str) + "/" + df["year"].astype(str)

    return df


if __name__ == "__main__":
    db_name = "databases/stage.db"
    table_name = "fires"
    df = read_sqlite(db_name, table_name)
    # print(df.head(5))
    # analyze_data(df)
    df = transform_state_name(df)
    df = agg_per_month(df)
    df = add_month_number(df)
    df = add_month_year(df)

    print(df.head(5))
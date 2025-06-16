from sklearn.linear_model import LinearRegression
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# A regressão linear é um modelo matemático que descreve a relação entre diversas variáveis.
# Os modelos de regressão linear são um procedimento estatístico que ajuda a prever o futuro.
# Ele é usado em campos científicos e nos negócios. Nas últimas décadas tem sido usado em Machine Learning.
# https://ebaconline.com.br/blog/regressao-linear-seo


def extract_data(db_name: str, table_name: str) -> pd.DataFrame:
    """
    Extrai os dadados do Data Warehouse.

    Args:
        db_name (str): Nome do banco de dados.
        table_name (str): Nome da tabela.
    Returns:
        pd.DataFrame: DataFrame do pandas com os dados lidos.
    """
    conn = sqlite3.connect(db_name)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    # Converte a coluna month_year para o formato datetime
    df["month_year"] = pd.to_datetime(df["month_year"], format="%m/%Y")
    conn.close()
    return df


def train_model(x: pd.DataFrame, y: pd.Series) -> LinearRegression:
    """
    Treina o modelo de regressão linear.

    Args:
        x (pd.DataFrame): DataFrame do pandas com os dados de entrada.
        y (pd.Series): Série do pandas com os dados de saída.

    Returns:
        LinearRegression: Modelo treinado.
    """
    model = LinearRegression()
    model.fit(x, y)
    print(f"Coeficientes: {model.coef_}")
    return model


def predict_by_state(df: pd.DataFrame, state: str) -> None:
    """
    Realiza a estimativa de queimadas para um estado específico.

    Args:
        df (pd.DataFrame): DataFrame do pandas com os dados.
        state (str): Nome do estado.
    """
    df = df[df["state"] == state]

    df = df.groupby(["year"]).agg({"number": "sum"}).reset_index()

    x = df[["year"]]
    y = df["number"]

    print(x, y)
    # Cria o modelo de regressão linear
    model = train_model(x, y)

    future_years = pd.DataFrame({"year": list(range(2017, 2026))})
    predictions = model.predict(future_years)

    # Mostrar previsões
    future_years["predicted_number"] = predictions
    print(future_years)

    plt.plot(df["year"], df["number"], label="Histórico")
    plt.plot(future_years["year"], future_years["predicted_number"], label="Previsão", linestyle="--")
    plt.xlabel("Ano")
    plt.ylabel("Número")
    plt.title(f"Regressão Linear - Estado: {state}")
    plt.legend()
    plt.grid(True)
    plt.show()


def predict_all_states(df: pd.DataFrame) -> None:
    """
    Realiza a estimativa de queimadas para todos os estados.

    Args:
        df (pd.DataFrame): DataFrame do pandas com os dados.
    """
    df = df.groupby(["year"]).agg({"number": "sum"}).reset_index()

    x = df[["year"]]
    y = df["number"]
    # Cria o modelo de regressão linear
    model = train_model(x, y)

    future_years = pd.DataFrame({"year": list(range(2017, 2026))})
    predictions = model.predict(future_years)

    # Mostrar previsões
    future_years["predicted_number"] = predictions
    print(future_years)

    plt.plot(df["year"], df["number"], label="Histórico")
    plt.plot(future_years["year"], future_years["predicted_number"], label="Previsão", linestyle="--")
    plt.xlabel("Ano")
    plt.ylabel("Número")
    plt.title("Regressão Linear - Todos os estados")
    plt.legend()
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    # Carrega os dados do Data Warehouse
    db_name = "databases/datawarehouse.db"
    table_name = "fires"
    df = extract_data(db_name, table_name)
    # Realiza a previsão para o estado do Tocantins
    predict_by_state(df, "Tocantins")
    # Realiza a previsão para todos os estados
    predict_all_states(df)
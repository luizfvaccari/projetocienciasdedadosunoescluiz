import extract
import transform
import load

if __name__ == "__main__":
    # -#-#-# Extract #-#-#-#
    # Define o caminho do arquivo CSV
    file_path = "data/acidentes.csv"
    # Extrai os dados do arquivo CSV
    data = extract.extract_data(file_path)
    # Realiza a exploração dos dados
    extract.data_exploration(data)
    # Cria a database SQLite
    db_name = "databases/stage.db"
    extract.create_database(db_name)
    # Cria a tabela
    table_name = "acidentes_industriais"
    extract.create_table(db_name, table_name)
    extract.insert_data(data, db_name, table_name)

    # -#-#-# Transform #-#-#-#
    # Le os dados da tabela SQLite
    stage_data = transform.read_sqlite(db_name, table_name)
    # Analisa os dados para verificar transformações necessárias
    transform.analyze_data(stage_data)
    # Transforma nomes dos estados
    stage_data = transform.transform_state_name(stage_data)
    # Agrega os dados por mês
    stage_data = transform.agg_per_month(stage_data)
    stage_data = transform.add_month_number(stage_data)
    stage_data = transform.add_month_year(stage_data)

    # -#-#-# Load #-#-#-#
    # Cria o datawarehouse
    db_name = "databases/datawarehouse.db"
    table_name = "industrial"
    load.create_database(db_name)
    # Cria a tabela e insere os dados
    load.create_table(db_name, table_name)
    load.insert_data(stage_data, db_name, table_name)
import extract
if __name__ == "__main__":
    file_path = "data/acidentes.csv"
    data = extract.extract_data(file_path) 
    extract.data_exploration(data) 

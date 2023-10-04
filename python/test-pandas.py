import pyarrow.flight as flight
import pandas as pd
import es_auth
import configparser


def main():
    config = configparser.ConfigParser()
    config.read("../config.ini")
    es_config = config["elasticsearch"]

    client = flight.connect("grpc://localhost:33333")
    client.authenticate(es_auth.basic(es_config["login"], es_config["password"]))

    esql = "from employees |  stats count = count(id) by language"

    stream = client.do_get(flight.Ticket(esql))
    table = stream.read_all()
    df: pd.DataFrame = table.to_pandas()

    max_id = df['count'].idxmax()
    max_row = df.iloc[max_id]

    print("language most spoken found at row", max_id, ": ", max_row['count'], " people speaking ", max_row['language'])


if __name__ == "__main__":
    main()

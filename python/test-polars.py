import pyarrow.flight as flight
import polars as ps
import auth_header
import configparser


def main():
    config = configparser.ConfigParser()
    config.read("../config.ini")
    es_config = config["elasticsearch"]

    auth_middleware = auth_header.basic(es_config["login"], es_config["password"])
    client = flight.connect("grpc://localhost:33333", middleware=[auth_middleware])

    esql = "from employees |  stats count = count(id) by language"
    stream = client.do_get(flight.Ticket(esql))

    table = stream.read_all()
    df: ps.DataFrame = ps.from_arrow(table)

    print(df)

    # max_id = df['x'].idxmax()
    # max_row = df.iloc[max_id]

    # print("language most spoken found at row", max_id, " - ", max_row['x'], " people speaking ", max_row['language.keyword'])


if __name__ == "__main__":
    main()

# elasticsearch-arrow-experiments
Experiments on exposing Elasticsearch as Apache Arrow


You need Elasticsearch with ES|QL to run this experiment. At the time of writing it's only available in 8.11 snapshots:

```
docker run --it --name es-esql -p 9200:9200 docker.elastic.co/elasticsearch/elasticsearch:8.11.0-SNAPSHOT`
```

Copy `config-example.ini` to `config.ini` and paste there the password displayed at the end of Elasticsearch's initialization sequence.

The examples need some test data. To seed the Elasticsearch server with random test data, run `./gradlew ingest-data`.

To start the ArrowFlight / ES|QL bridge, run `./gradlew flight-server`.

**Java example:** run `./gradlew flight-client`

**Python examples:** the `python` directory has examples with both Pandas and Polars. They also illustrate two different authentication methods: one using Arrow Flight's native authentication, and one using a middleware that sets the http `Authorization` header. The bridge server understands both.

**Rust example:** the `rust` directory has an exemple with Arrow.

## ESQL serialization benchmarks

In the Java project the `benchmarks` package contains benchmarks to compare ESQL serialization to JSON, CBOR and Arrow.

**Size benchmark**: CBOR and Arrow produce payload sizes of comparable size. In Arrow number columns are more compact, but string take a bit more space (size is always a 32 bits integer).

**Memory benchmark**: TODO. Arrow libraries are just dataframe wrappers around the byte buffer. So contrarily to CBOR, Arrow has zero deserialization cost.

**CPU benchmark**: TODO. Since Arrow has no deserialization cost, we can reasonably expect CPU usage to be minimal. Arrow libraries also come with they own optimized computation kernels, something a user would have to bring/write with CBOR.

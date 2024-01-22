package co.elastic.benchmarks;

import co.elastic.es_flight.esql.ESQLArrowResponse;
import co.elastic.es_flight.esql.ESQLResponse;
import co.elastic.es_flight.esql.ESQLResponse.ColumnType;
import co.elastic.es_flight.esql.ESQLResponseGenerator;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

public class SizeBenchmark {

    public static final BufferAllocator allocator;

    static {
        allocator = new RootAllocator(RootAllocator.configBuilder()
            .allocationManagerFactory(DefaultAllocationManagerFactory.FACTORY)
            .build()
        );
    }

    private static final Map<String, Function<ESQLResponse, byte[]>> encoders = Map.of(
        "json", SizeBenchmark::toJSON,
        "cbor", SizeBenchmark::toCBOR,
        "arrow-stream", SizeBenchmark::toArrowStream,
        "arrow-file", SizeBenchmark::toArrowFile
    );

    public static void main(String[] args) throws IOException {
        var rnd = new Random(0);
        //var esql = ESQLResponseGenerator.generate(20, 1000, rnd);
        var esql = ESQLResponseGenerator.generate(10000, 0.0f, rnd,
            ColumnType.BOOLEAN, ColumnType.INTEGER, ColumnType.FLOAT, ColumnType.FLOAT, ColumnType.KEYWORD
            //ColumnType.BOOLEAN, ColumnType.INTEGER, ColumnType.FLOAT, ColumnType.DOUBLE
        );
        var names = encoders.keySet().stream().sorted().toList();
        for (var name: names) {
            var encoded = encoders.get(name).apply(esql);
            var baos = new ByteArrayOutputStream();
            var gzout = new GZIPOutputStream(baos);
            gzout.write(encoded);
            gzout.close();

            double gzipRatio = (double) baos.size()/encoded.length;

            System.out.printf("%s - %d kiB, gzipped: %d kiB (%.0f%%)\n",
                name, encoded.length/1024, baos.size()/1024, gzipRatio*100
            );
        }
    }

    public static byte[] toJSON(ESQLResponse esql) {
        var factory = JsonFactory.builder()
            .build();

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        try (
            var out = new ByteArrayOutputStream();
            var generator = factory.createGenerator(out);
        ) {
            mapper.writeValue(generator, esql);
            generator.close();
            return out.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static byte[] toCBOR(ESQLResponse esql) {
        var factory = CBORFactory.builder()
            // Note that Elasticsearch doesn't have these
            .enable(CBORGenerator.Feature.WRITE_MINIMAL_INTS)
            .enable(CBORGenerator.Feature.WRITE_MINIMAL_DOUBLES)
            .build();

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        try (
            var out = new ByteArrayOutputStream();
            var generator = factory.createGenerator(out);
        ) {
            mapper.writeValue(generator, esql);
            generator.close();
            return out.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    // See https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format
    public static byte[] toArrowStream(ESQLResponse esql) {

        VectorSchemaRoot root = ESQLArrowResponse.toArrow(esql, allocator);

        try (
            var out = new ByteArrayOutputStream();
            var writer = new ArrowStreamWriter(root, /*DictionaryProvider=*/null, Channels.newChannel(out)
                // Compresses each column, doesn't bring significant benefits
                //, IpcOption.DEFAULT, CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.LZ4_FRAME
            );
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();

            writer.close();
            return out.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    // See https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-random-access-files
    public static byte[] toArrowFile(ESQLResponse esql) {

        VectorSchemaRoot root = ESQLArrowResponse.toArrow(esql, allocator);

        try (
            var out = new ByteArrayOutputStream();
            var writer = new ArrowFileWriter(root, /*DictionaryProvider=*/null, Channels.newChannel(out));
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();

            writer.close();
            return out.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}

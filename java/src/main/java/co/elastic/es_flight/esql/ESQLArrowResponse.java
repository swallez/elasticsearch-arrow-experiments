package co.elastic.es_flight.esql;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;
import java.util.stream.Collectors;

public class ESQLArrowResponse {

    public static Schema createSchema(List<ESQLResponse.Column> columns) {
        List<Field> fields = columns.stream()
            .map(col -> {
                ArrowType type = switch (col.type()) {
                    case TEXT, KEYWORD -> new ArrowType.Utf8();
                    case INTEGER -> new ArrowType.Int(32, true);
                    case LONG -> new ArrowType.Int(64, true);
                    case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
                    case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                    case BOOLEAN -> new ArrowType.Bool();
                };

                return new Field(col.name(), new FieldType(true, type, null), null);
            })
            .collect(Collectors.toList());

        return new Schema(fields);
    }

    public static void columnsToVectors(ESQLResponse response, VectorSchemaRoot root) {
        final var colCount = response.columns().size();
        final var rowCount = response.values().get(0).size();

        // Vector lifecycle: see https://arrow.apache.org/docs/java/vector.html#vector-life-cycle

        for (var colIdx = 0; colIdx < colCount; colIdx++) {
            var colDef = response.columns().get(colIdx);
            var colValues = response.values().get(colIdx);

            var rawVector = root.getVector(colIdx);
            rawVector.setInitialCapacity(rowCount);
            rawVector.allocateNew();

            Void v = switch (colDef.type()) {
                case INTEGER -> {
                    var vector = (IntVector) rawVector;
                    for (var i = 0; i < rowCount; i++) {
                        vector.set(i, colValues.get(i).intValue());
                    }
                    yield null;
                }

                case LONG -> {
                    var vector = (BigIntVector) rawVector;
                    for (var i = 0; i < rowCount; i++) {
                        vector.set(i, colValues.get(i).longValue());
                    }
                    yield null;
                }

                case FLOAT -> {
                    var vector = (Float4Vector) rawVector;
                    for (var i = 0; i < rowCount; i++) {
                        vector.set(i, colValues.get(i).floatValue());
                    }
                    yield null;
                }

                case DOUBLE -> {
                    var vector = (Float8Vector) rawVector;
                    for (var i = 0; i < rowCount; i++) {
                        vector.set(i, colValues.get(i).doubleValue());
                    }
                    yield null;
                }

                case TEXT, KEYWORD -> {
                    var vector = (VarCharVector) rawVector;
                    for (var i = 0; i < rowCount; i++) {
                        vector.setSafe(i, new Text(colValues.get(i).asText()));
                    }
                    yield null;
                }

                case BOOLEAN -> {
                    var vector = (BitVector) rawVector;
                    for (var i = 0; i < rowCount; i++) {
                        vector.set(i, colValues.get(i).booleanValue() ? 1 : 0);
                    }
                    yield null;
                }
            };
        }

        // Will call setValueCount on all vectors
        root.setRowCount(rowCount);
    }

    public static VectorSchemaRoot toArrow(ESQLResponse response, BufferAllocator allocator) {
        var schema = createSchema(response.columns());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        columnsToVectors(response, root);
        return root;
    }

    public static void sendFlightResponse(ESQLResponse response, BufferAllocator allocator, FlightProducer.ServerStreamListener listener) {
        var schema = createSchema(response.columns());

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        listener.start(root);

        columnsToVectors(response, root);

        listener.putNext();
        listener.completed();
        root.close();
    }
}

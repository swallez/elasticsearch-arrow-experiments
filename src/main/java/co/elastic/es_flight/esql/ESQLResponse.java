package co.elastic.es_flight.esql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class ESQLResponse {

    public List<Column> columns;
    public List<List<JsonNode>> values;

    public static class Column {
        public String name;
        public String type;
    }

    public void toArrow(BufferAllocator allocator, FlightProducer.ServerStreamListener listener) {

        // Build the schema
        List<Field> fields = columns.stream()
            .map(col -> {
                ArrowType type = switch (col.type) {
                    case "text", "keyword" -> new ArrowType.Utf8();
                    case "long" -> new ArrowType.Int(64, true);
                    default -> throw new RuntimeException("Unhandled ESQL type " + col.type);
                };

                return new Field(col.name, new FieldType(true, type, null), null);
            })
            .collect(Collectors.toList());

        var schema = new Schema(fields);

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        listener.start(root);

        int rowCount = values.size();
        System.out.println("Rows: " + rowCount);

        for (int col = 0; col < fields.size(); col++) {
            var field = fields.get(col);
            switch (field.getType().getTypeID()) {
                case Int -> {
                    BigIntVector vector = (BigIntVector) root.getVector(field);
                    vector.allocateNew(rowCount);
                    for (int row = 0; row < rowCount; row++) {
                        vector.set(row, ((NumericNode)values.get(row).get(col)).longValue());
                    }
                }
                case Utf8 -> {
                    VarCharVector vector = (VarCharVector)root.getVector(field);
                    vector.allocateNew(rowCount);
                    for (int row = 0; row < rowCount; row++) {
                        vector.set(row, values.get(row).get(col).toString().getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
        }

        root.setRowCount(rowCount);

        listener.putNext();
        listener.completed();
    }

}

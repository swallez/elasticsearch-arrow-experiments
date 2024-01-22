package co.elastic.es_flight.esql;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import jakarta.json.JsonNumber;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.text.RandomStringGenerator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public record ESQLResponse(List<Column> columns, List<List<JsonNode>> values) {

    public record Column(String name, ColumnType type) {}

    public enum ColumnType {
        @JsonProperty("integer")
        INTEGER,
        @JsonProperty("long")
        LONG,
        @JsonProperty("float")
        FLOAT,
        @JsonProperty("double")
        DOUBLE,
        @JsonProperty("text")
        TEXT,
        @JsonProperty("keyword")
        KEYWORD,
        @JsonProperty("boolean")
        BOOLEAN;
    }

//    public void toArrow1(BufferAllocator allocator, FlightProducer.ServerStreamListener listener) {
//
//        // Build the schema
//        List<Field> fields = columns.stream()
//            .map(col -> {
//                ArrowType type = switch (col.type) {
//                    case TEXT, KEYWORD -> new ArrowType.Utf8();
//                    case LONG -> new ArrowType.Int(64, true);
//                    case INTEGER -> new ArrowType.Int(32, true);
//                    case BOOLEAN -> new ArrowType.Bool();
//                };
//
//                return new Field(col.name, new FieldType(true, type, null), null);
//            })
//            .collect(Collectors.toList());
//
//        var schema = new Schema(fields);
//
//        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
//        listener.start(root);
//
//        int rowCount = values.size();
//        System.out.println("Rows: " + rowCount);
//
//        for (int col = 0; col < fields.size(); col++) {
//            var field = fields.get(col);
//            switch (field.getType().getTypeID()) {
//                case Int -> {
//                    BigIntVector vector = (BigIntVector) root.getVector(field);
//                    vector.allocateNew(rowCount);
//                    for (int row = 0; row < rowCount; row++) {
//                        vector.set(row, ((NumericNode)values.get(row).get(col)).longValue());
//                    }
//                }
//                case Utf8 -> {
//                    VarCharVector vector = (VarCharVector)root.getVector(field);
//                    vector.allocateNew(rowCount);
//                    for (int row = 0; row < rowCount; row++) {
//                        vector.set(row, values.get(row).get(col).toString().getBytes(StandardCharsets.UTF_8));
//                    }
//                }
//            }
//        }
//
//        root.setRowCount(rowCount);
//
//        listener.putNext();
//        listener.completed();
//    }

}

package co.elastic.es_flight.esql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ESQLResponseGenerator {

    private static final ESQLResponse.ColumnType[] COLUMN_TYPES = ESQLResponse.ColumnType.values();

    public static ESQLResponse generate(final int rowCount, float nullProbability, Random rnd, ESQLResponse.ColumnType... types) {
        var colCount = types.length;
        var columns = new ArrayList<ESQLResponse.Column>(colCount);
        for (var colIdx = 0; colIdx < colCount; colIdx++) {
            columns.add(new ESQLResponse.Column("col-" + colIdx, types[colIdx]));
        }
        var values = generateValues(columns, rowCount, nullProbability, rnd);
        return new ESQLResponse(columns, values);
    }

    public static ESQLResponse generate(final int colCount, final int rowCount, Random rnd) {
        var types = new ESQLResponse.ColumnType[colCount];
        for (var colIdx = 0; colIdx < colCount; colIdx++) {
            types[colIdx] =  COLUMN_TYPES[rnd.nextInt(COLUMN_TYPES.length)];
        }
        return generate(rowCount, 0, rnd, types);
    }

    public static List<List<JsonNode>> generateValues(List<ESQLResponse.Column> columns, int rowCount, float nullProbablity, Random rnd) {
        var table = new ArrayList<List<JsonNode>>(columns.size());

        for (ESQLResponse.Column column : columns) {
            var type = column.type();
            var colValues = new ArrayList<JsonNode>(rowCount);
            for (var rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                if (nullProbablity != 0 && rnd.nextFloat() <= nullProbablity) {
                    colValues.add(NullNode.getInstance());
                } else {
                    colValues.add(nextValue(type, rnd));
                }
            }
            table.add(colValues);
        }
        return table;
    }

    public static JsonNode nextValue(ESQLResponse.ColumnType type, Random rnd) {
        return switch (type) {
            case INTEGER -> new IntNode(rnd.nextInt());
            case LONG -> new LongNode(rnd.nextLong());
            case FLOAT -> new FloatNode(rnd.nextFloat());
            case DOUBLE -> new DoubleNode(rnd.nextDouble());
            case TEXT -> generateString(15, 50, rnd);
            case KEYWORD -> generateString(3, 20, rnd);
            case BOOLEAN -> BooleanNode.valueOf(rnd.nextBoolean());
        };
    }

    private static TextNode generateString(int minLen, int maxLen, Random rnd) {
        int len = rnd.nextInt(minLen, maxLen);

        StringBuilder sb = new StringBuilder(len);
        for (var i = 0; i < len; i++) {
            sb.append((char)rnd.nextInt(0x20, 0x7F));
        }
        return new TextNode(sb.toString());
    }
}

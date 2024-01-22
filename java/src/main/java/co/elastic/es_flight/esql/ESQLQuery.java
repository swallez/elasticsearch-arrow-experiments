package co.elastic.es_flight.esql;

/** A simple ESQL query with choice of colum/row format */
public record ESQLQuery(String query, boolean columnar) {
};

package com.adelegue.akka.jdbc.query;

import akka.japi.Procedure;
import akka.stream.javadsl.Flow;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import com.adelegue.akka.jdbc.stream.stage.AtTheEndStage;
import scala.concurrent.Future;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by adelegue on 07/05/2016.
 */
public class AtTheEndBuilder {

    private final List<Procedure<SqlConnection>> handlers ;
    private final Future<SqlConnection> sqlConnection;


//    public AtTheEndBuilder(Procedure<SqlConnection> handler, Future<SqlConnection> sqlConnection) {
//        this.handlers = Collections.singletonList(handler);
//        this.sqlConnection = sqlConnection;
//    }

    public AtTheEndBuilder(List<Procedure<SqlConnection>> handlers, Future<SqlConnection> sqlConnection) {
        this.handlers = handlers;
        this.sqlConnection = sqlConnection;
    }


    public AtTheEndBuilder then(Procedure<SqlConnection> handler) {
        return new AtTheEndBuilder(Stream.concat(handlers.stream(), Stream.of(handler)).collect(Collectors.toList()), sqlConnection);
    }

    public AtTheEndBuilder and(Procedure<SqlConnection> handler) {
        return new AtTheEndBuilder(Stream.concat(handlers.stream(), Stream.of(handler)).collect(Collectors.toList()), sqlConnection);
    }

    public <T> Flow<T, T, ?> apply() {
        return Flow.fromGraph(new AtTheEndStage<>(handlers, sqlConnection));
    }

}

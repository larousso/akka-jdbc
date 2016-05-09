package com.adelegue.akka.jdbc.query;

import akka.japi.Procedure;
import akka.stream.javadsl.Flow;
import com.adelegue.akka.jdbc.Sql;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import com.adelegue.akka.jdbc.function.F;
import com.adelegue.akka.jdbc.stream.stage.OnEachStage;
import scala.concurrent.Future;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by adelegue on 07/05/2016.
 */
public class DoOnEachBuilder<T> {

    private final List<F.BiConsumerUnchecked<T, SqlConnection>> handlers ;
    private final Future<SqlConnection> sqlConnection;

    public DoOnEachBuilder(List<F.BiConsumerUnchecked<T, SqlConnection>> handlers, Future<SqlConnection> sqlConnection) {
        this.handlers = handlers;
        this.sqlConnection = sqlConnection;
    }


    public DoOnEachBuilder<T> then(Sql.ConnectionAction handler) {
        return new DoOnEachBuilder<>(Stream.concat(handlers.stream(), Stream.of(handler).map(Sql::<T>toBiAction)).collect(Collectors.toList()), sqlConnection);
    }

    public DoOnEachBuilder<T> and(Sql.ConnectionAction handler) {
        return then(handler);
    }

    public DoOnEachBuilder<T> then(F.BiConsumerUnchecked<T, SqlConnection> handler) {
        return new DoOnEachBuilder<>(Stream.concat(handlers.stream(), Stream.of(handler)).collect(Collectors.toList()), sqlConnection);
    }

    public DoOnEachBuilder<T> and(F.BiConsumerUnchecked<T, SqlConnection> handler) {
        return then(handler);
    }

    public Flow<T, T, ?> apply() {
        return Flow.fromGraph(new OnEachStage<>(handlers, sqlConnection));
    }

}

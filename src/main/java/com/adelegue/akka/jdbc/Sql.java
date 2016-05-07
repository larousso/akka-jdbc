package com.adelegue.akka.jdbc;

import akka.actor.ActorSystem;
import akka.japi.Procedure;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import com.adelegue.akka.jdbc.function.F;
import com.adelegue.akka.jdbc.query.*;
import com.adelegue.akka.jdbc.utils.ResultSetExtractor;
import scala.Function1;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.adelegue.akka.jdbc.exceptions.ExceptionsHandler.handleChecked0;
import static com.adelegue.akka.jdbc.utils.AkkaUtils.getExecutionContext;
import static com.adelegue.akka.jdbc.utils.ScalaBridge.func;

/**
 * Created by adelegue on 29/04/2016.
 */
public class Sql {

    final Future<SqlConnection> connection;

    final ActorSystem actorSystem;

    final Optional<String> dispatcher;

    final ExecutionContext executionContext;

    Sql(Future<SqlConnection> connection, ActorSystem actorSystem, Optional<String> dispatcher) {
        this.connection = connection;
        this.actorSystem = actorSystem;
        this.dispatcher = dispatcher;
        this.executionContext = getExecutionContext(actorSystem, dispatcher);
    }

    public Sql keepConnectionOpened() {
        Function1<SqlConnection, SqlConnection> func = func(c -> new SqlConnection(c.connection(), c.name(), true));
        return new Sql(connection.map(func, this.executionContext), actorSystem, dispatcher);
    }

    public Sql withTransaction() {
        return new Sql(connection.map(func(c -> {
            handleChecked0(() -> c.connection().setAutoCommit(false));
            return c;
        }), this.executionContext), actorSystem, dispatcher);
    }

    //Sources

    public Source<SqlConnection, ?> connection() {
        return Source.fromFuture(connection);
    }

    public SelectQueryBuilder<ResultSet> select(String query) {
        return new SelectQueryBuilder<>(query, connection.map(func(c -> new SqlContext(actorSystem, c, Optional.empty())), this.executionContext), ResultSetExtractor.identity());
    }

    public Source<SqlConnection, ?> beginTransaction() {
        return Source.fromFuture(connection.map(func(c -> {
            handleChecked0(() -> c.connection().setAutoCommit(false));
            return c;
        }), this.executionContext));
    }

    public UpdateQueryBuilder update(String query) {
        return new UpdateQueryBuilder(query, connection.map(func(c -> new SqlContext(actorSystem, c, Optional.empty())), this.executionContext));
    }

    public <T> Flow<T, T, ?> atTheEnd(ConnectionAction... handlers) {
        return new AtTheEndBuilder(Arrays.asList(handlers), connection).apply();
    }

    public AtTheEndBuilder atTheEnd() {
        return new AtTheEndBuilder(Collections.emptyList(), connection);
    }

    public <T> Flow<T, T, ?> doOnEachWithInParam(ConnectionBiAction<T>... handlers) {
        return new DoOnEachBuilder<>(Arrays.asList(handlers), connection).apply();
    }

    public <T> Flow<T, T, ?> doOnEach(ConnectionAction... handlers) {
        List<F.BiConsumerUnchecked<T, SqlConnection>> collect = Arrays.asList(handlers).stream().map(e -> Sql.<T>toBiAction(e)).collect(Collectors.toList());
        return new DoOnEachBuilder<>(collect, connection).apply();
    }

    public DoOnEachBuilder doOnEach() {
        return new DoOnEachBuilder<>(Collections.emptyList(), connection);
    }

    public static <In> Flow<In, List<In>, ?> toList() {
        return Flow.<In>create().fold(new ArrayList<>(), (acc, elt) -> Stream.concat(acc.stream(), Stream.of(elt)).collect(Collectors.toList()));
    }

    public static <In, Out> Flow<In, Out, ?> empty() {
        return Flow.<In>create().fold(0, (acc, elt) -> acc+1).flatMapMerge(1, any -> Source.empty());
    }

    public static <In, Out> Flow<In, Out, ?> andThen(SelectQueryBuilder<Out> builder) {
        return Flow.<In>create().flatMapMerge(1, in -> builder.get());
    }

    public static <In> Flow<In, Integer, ?> andThen(UpdateQueryBuilder builder) {
        return Flow.<In>create().via(builder.toFlow());
    }

    public static <In, Out> Flow<In, Out, ?> andThen(UpdateQueryGeneratedKeyBuilder<Out> builder) {
        return Flow.<In>create().flatMapMerge(1, in -> builder.get());
    }

    public static <In, Out> Flow<In, Out, ?> andThen(Source<Out, ?> source) {
        return Flow.<In>create().flatMapMerge(1, any -> source);
    }

    public static void commit(SqlConnection connection) throws Exception {
        connection.connection().commit();
    }

    public static ConnectionAction commit() throws Exception {
        return Sql::commit;
    }

    public static <T> ConnectionBiAction<T> doAndCommit(Procedure<T> action) throws Exception {
        return (elt, conn) -> {
            action.apply(elt);
            commit(conn);
        };
    }

    public static void endTransaction(SqlConnection connection) throws Exception {
        connection.connection().setAutoCommit(true);
    }

    public static ConnectionAction endTransaction() {
        return Sql::endTransaction;
    }

    public static <T> ConnectionBiAction<T> doAndEndTransaction(Procedure<T> action) throws Exception {
        return (elt, conn) -> {
            action.apply(elt);
            endTransaction(conn);
        };
    }

    public static void closeConnection(SqlConnection connection) throws Exception {
        connection.connection().close();
    }

    public static ConnectionAction  closeConnection() {
        return Sql::closeConnection;
    }

    public static <T> ConnectionBiAction<T> doAndCloseConnection(Procedure<T> action) throws Exception {
        return (elt, conn) -> {
            action.apply(elt);
            closeConnection(conn);
        };
    }

    public static void rollback(SqlConnection connection) throws Exception {
        connection.connection().rollback();
    }

    public static <T> ConnectionBiAction<T> doAndRollback(Procedure<T> action) throws Exception {
        return (elt, conn) -> {
            action.apply(elt);
            rollback(conn);
        };
    }

    //Getter
    public Future<SqlConnection> getConnection() {
        return connection;
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    public interface ConnectionAction extends Procedure<SqlConnection> {}
    public interface ConnectionBiAction<T> extends F.BiConsumerUnchecked<T, SqlConnection> {}

    static <T> ConnectionBiAction<T> toBiAction(ConnectionAction action) {
        return (any, sqlConnection) -> action.apply(sqlConnection);
    }

}

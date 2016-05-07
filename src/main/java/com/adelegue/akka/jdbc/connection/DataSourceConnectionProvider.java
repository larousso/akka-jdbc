package com.adelegue.akka.jdbc.connection;

import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import com.adelegue.akka.jdbc.exceptions.ExceptionsHandler;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import javax.sql.DataSource;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by adelegue on 30/04/2016.
 */
public class DataSourceConnectionProvider implements ConnectionProvider {

    final DataSource dataSource;

    final AtomicInteger connectionNumber = new AtomicInteger();

    final ActorSystem actorSystem;

    final String dispatcher;

    public DataSourceConnectionProvider(DataSource dataSource, ActorSystem actorSystem, String dispatcher) {
        this.dataSource = dataSource;
        this.actorSystem = actorSystem;
        this.dispatcher = dispatcher;
    }

    @Override
    public Future<SqlConnection> getConnection() {
        Optional<ExecutionContext> messageDispatcher = Optional.ofNullable(dispatcher).map(actorSystem.dispatchers()::lookup);
        return Futures.future(() ->
                ExceptionsHandler.handleChecked(() -> new SqlConnection(dataSource.getConnection(), "AkkaJdbcConnection" + connectionNumber.getAndIncrement(), false))
        , messageDispatcher.orElse(actorSystem.dispatcher()));
    }
}

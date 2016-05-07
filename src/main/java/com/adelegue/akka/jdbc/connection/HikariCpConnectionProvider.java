package com.adelegue.akka.jdbc.connection;

import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import com.adelegue.akka.jdbc.exceptions.ExceptionsHandler;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by adelegue on 07/05/2016.
 */
public class HikariCpConnectionProvider implements ConnectionProvider {

    final HikariDataSource hikariDataSource;

    final AtomicInteger connectionNumber = new AtomicInteger();

    final ActorSystem actorSystem;

    final Optional<String> dispatcher;

    public HikariCpConnectionProvider(HikariDataSource hikariDataSource, ActorSystem actorSystem, Optional<String> dispatcher) {
        this.hikariDataSource = hikariDataSource;
        this.actorSystem = actorSystem;
        this.dispatcher = dispatcher;
    }

    public HikariCpConnectionProvider(HikariConfig hikariConfig, ActorSystem actorSystem, Optional<String> dispatcher) {
        this(new HikariDataSource(hikariConfig), actorSystem, dispatcher);
    }

    public HikariCpConnectionProvider(Properties hikariConfig, ActorSystem actorSystem, Optional<String> dispatcher) {
        this(new HikariConfig(hikariConfig), actorSystem, dispatcher);
    }

    @Override
    public Future<SqlConnection> getConnection() {
        Optional<ExecutionContext> messageDispatcher = dispatcher.map(actorSystem.dispatchers()::lookup);
        return Futures.future(() ->
                        ExceptionsHandler.handleChecked(() -> new SqlConnection(hikariDataSource.getConnection(), "AkkaJdbcConnection" + connectionNumber.getAndIncrement(), false))
                , messageDispatcher.orElse(actorSystem.dispatcher()));
    }
}

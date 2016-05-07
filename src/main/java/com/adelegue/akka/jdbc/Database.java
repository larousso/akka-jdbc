package com.adelegue.akka.jdbc;

import akka.actor.ActorSystem;
import com.adelegue.akka.jdbc.connection.ConnectionProvider;
import com.adelegue.akka.jdbc.connection.DataSourceConnectionProvider;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import com.adelegue.akka.jdbc.connection.UniqueConnectionProvider;
import scala.concurrent.Future;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.adelegue.akka.jdbc.utils.AkkaUtils.getExecutionContext;
import static com.adelegue.akka.jdbc.utils.ScalaBridge.func;

/**
 * Created by adelegue on 30/04/2016.
 */
public class Database {


    final ActorSystem actorSystem;

    final ConnectionProvider connectionProvider;

    static final AtomicInteger connectionNumber = new AtomicInteger();

        final Optional<String> dispatcher;

    public Database(ConnectionProvider connectionProvider, ActorSystem actorSystem, Optional<String> dispatcher) {
        this.connectionProvider = connectionProvider;
        this.actorSystem = actorSystem;
        this.dispatcher = dispatcher;
    }

    public static Database from(DataSource dataSource, ActorSystem actorSystem, Optional<String> dispatcher) {
        return new Database(new DataSourceConnectionProvider(dataSource, actorSystem, dispatcher), actorSystem, dispatcher);
    }

    public static Database from(DataSource dataSource, ActorSystem actorSystem) {
        return new Database(new DataSourceConnectionProvider(dataSource, actorSystem, Optional.empty()), actorSystem, Optional.empty());
    }

    public static Database from(DataSource dataSource) {
        ActorSystem actorSystem = ActorSystem.create("Akka-Jdbc");
        return new Database(new DataSourceConnectionProvider(dataSource, actorSystem, Optional.empty()), actorSystem, Optional.empty());
    }

    public static Database from(Connection connection, ActorSystem actorSystem) {
        return new Database(new UniqueConnectionProvider(new SqlConnection(connection, nameConnection(), false)), actorSystem, Optional.empty());
    }

    public static Database from(Connection connection) {
        return new Database(new UniqueConnectionProvider(new SqlConnection(connection, nameConnection(), false)), ActorSystem.create("Akka-Jdbc"), Optional.empty());
    }

    private static String nameConnection() {
        return String.format("AkkaJdbcConnection%d", connectionNumber.getAndIncrement());
    }

    public Sql sql() {
        return new Sql(connectionProvider.getConnection(), actorSystem, this.dispatcher);
    }

    public Sql sql(Boolean keepConnectionOpened) {
        Future<SqlConnection> connection = connectionProvider.getConnection().map(func(c -> new SqlConnection(c.connection(), c.name(), true)), getExecutionContext(actorSystem, dispatcher));
        return new Sql(connection, actorSystem, this.dispatcher);
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }
}

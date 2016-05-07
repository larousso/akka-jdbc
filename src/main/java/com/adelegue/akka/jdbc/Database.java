package com.adelegue.akka.jdbc;

import akka.actor.ActorSystem;
import com.adelegue.akka.jdbc.connection.*;
import com.zaxxer.hikari.HikariConfig;
import scala.concurrent.Future;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.adelegue.akka.jdbc.utils.AkkaUtils.getExecutionContext;
import static com.adelegue.akka.jdbc.utils.ScalaBridge.func;

/**
 * Created by adelegue on 30/04/2016.
 */
public class Database {


    private final ActorSystem actorSystem;

    private final ConnectionProvider connectionProvider;

    private static final AtomicInteger connectionNumber = new AtomicInteger();

    private final Optional<String> dispatcher;

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

    public static Database from(Properties hikariConfig) {
        return new Database(new HikariCpConnectionProvider(hikariConfig, ActorSystem.create("Akka-Jdbc"), Optional.empty()), ActorSystem.create("Akka-Jdbc"), Optional.empty());
    }

    public static Database from(HikariConfig hikariConfig) {
        return new Database(new HikariCpConnectionProvider(hikariConfig, ActorSystem.create("Akka-Jdbc"), Optional.empty()), ActorSystem.create("Akka-Jdbc"), Optional.empty());
    }

    public static DatabaseBuilder builder() {
        return new DatabaseBuilder();
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

    public static class DatabaseBuilder {

        String url;
        String username;
        String password;
        Integer minPoolSize;
        Integer maxPoolSize;
        Class dataSourceClass;
        ActorSystem actorSystem;
        String dispatcher;

        public DatabaseBuilder withUrl(String url) {
            this.url = url;
            return this;
        }

        public DatabaseBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public DatabaseBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public DatabaseBuilder withMinPoolSize(Integer minPoolSize) {
            this.minPoolSize = minPoolSize;
            return this;
        }

        public DatabaseBuilder withMaxPoolSize(Integer maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }

        public DatabaseBuilder withDataSourceClass(Class dataSourceClass) {
            this.dataSourceClass = dataSourceClass;
            return this;
        }

        public DatabaseBuilder withActorSystem(ActorSystem actorSystem) {
            this.actorSystem = actorSystem;
            return this;
        }

        public DatabaseBuilder withDispatcher(String dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        public Database build() {
            HikariConfig hikariConfig = new HikariConfig();
            if(url != null) {
                hikariConfig.setJdbcUrl(url);
            }
            hikariConfig.setDataSourceClassName(dataSourceClass.getName());
            hikariConfig.setUsername(username);
            hikariConfig.setPassword(password);
            hikariConfig.setMinimumIdle(minPoolSize);
            hikariConfig.setMaximumPoolSize(maxPoolSize);
            return Database.from(hikariConfig);
        }

    }
}

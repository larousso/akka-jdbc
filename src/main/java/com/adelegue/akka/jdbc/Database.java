package com.adelegue.akka.jdbc;

import akka.actor.ActorSystem;
import com.adelegue.akka.jdbc.connection.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariConfig;
import scala.concurrent.Future;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.adelegue.akka.jdbc.utils.AkkaUtils.getExecutionContext;
import static com.adelegue.akka.jdbc.utils.ScalaBridge.func;

/**
 * Created by adelegue on 30/04/2016.
 */
public class Database {

    private static final String DEFAULT_DISPATCHER = "akka.jdbc-dispatcher";

    private final ActorSystem actorSystem;

    private final ConnectionProvider connectionProvider;

    private static final AtomicInteger connectionNumber = new AtomicInteger();

    private final String dispatcher;

    private Database(ConnectionProvider connectionProvider, ActorSystem actorSystem, String dispatcher) {
        this.connectionProvider = connectionProvider;
        this.actorSystem = actorSystem;
        this.dispatcher = Optional.ofNullable(dispatcher).orElse(DEFAULT_DISPATCHER);
    }

    public static Database from(DataSource dataSource, ActorSystem actorSystem, String dispatcher) {
        return new Database(new DataSourceConnectionProvider(dataSource, actorSystem, dispatcher), actorSystem, dispatcher);
    }

    public static Database from(DataSource dataSource, ActorSystem actorSystem) {
        return from(dataSource, actorSystem, DEFAULT_DISPATCHER);
    }

    public static Database from(DataSource dataSource, String dispatcher, Integer threadPoolSize) {
        return from(dataSource, defaultSystem(threadPoolSize), dispatcher);
    }

    public static Database from(DataSource dataSource) {
        return from(dataSource, null, (Integer) null);
    }

    public static Database from(Connection connection, String dispatcher, Integer threadPoolsize) {
        return new Database(new UniqueConnectionProvider(new SqlConnection(connection, nameConnection(), false)), defaultSystem(threadPoolsize), dispatcher);
    }

    public static Database from(Connection connection, ActorSystem actorSystem) {
        return new Database(new UniqueConnectionProvider(new SqlConnection(connection, nameConnection(), false)), actorSystem, DEFAULT_DISPATCHER);
    }

    public static Database from(Connection connection) {
        return from(connection, defaultSystem());
    }

    public static Database from(Properties hikariConfig) {
        return from(new HikariConfig(hikariConfig));
    }

    public static Database from(Properties hikariConfig, String dispatcher, Integer threadPoolSize) {
        return from(new HikariConfig(hikariConfig), dispatcher, threadPoolSize);
    }

    public static Database from(HikariConfig hikariConfig) {
        return from(hikariConfig, defaultSystem(), DEFAULT_DISPATCHER);
    }

    public static Database from(HikariConfig hikariConfig, String dispatcher, Integer threadPoolSize) {
        return from(hikariConfig, defaultSystem(threadPoolSize), dispatcher);
    }

    public static Database from(HikariConfig hikariConfig, ActorSystem actorSystem, String dispatcher) {
        return new Database(new HikariCpConnectionProvider(hikariConfig, actorSystem, dispatcher), actorSystem, dispatcher);
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

    public ActorSystem getActorSystem() {
        return actorSystem;
    }


    private static ActorSystem defaultSystem() {
        return defaultSystem(null);
    }

    private static ActorSystem defaultSystem(Integer threadPoolSize) {
        if(threadPoolSize != null) {
            Config config = ConfigFactory.parseString("akka.jdbc-dispatcher.thread-pool-executor.fixed-pool-size = "+threadPoolSize);
            return ActorSystem.create("Akka-Jdbc", config);
        } else {
            return ActorSystem.create("Akka-Jdbc");
        }
    }

    public static class DatabaseBuilder {

        private String url;
        private String username;
        private String password;
        private Integer minPoolSize;
        private Integer maxPoolSize;
        private Class dataSourceClass;
        private ActorSystem actorSystem;
        private String dispatcher;
        private Integer threadPoolSize;
        private HikariConfig hikariConfig;

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

        public DatabaseBuilder withThreadPoolSize(Integer threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public DatabaseBuilder withHikariConfig(HikariConfig hikariConfig) {
            this.hikariConfig = hikariConfig;
            return this;
        }

        public Database build() {
            HikariConfig hikariConfig = Optional.ofNullable(this.hikariConfig).orElse(buildHikariConfig());
            return Optional
                    .ofNullable(actorSystem).map(system -> Database.from(hikariConfig, system, dispatcher))
                    .orElse(Database.from(hikariConfig, dispatcher, threadPoolSize));

        }

        private HikariConfig buildHikariConfig() {
            HikariConfig hikariConfig = new HikariConfig();
            apply(url, hikariConfig::setJdbcUrl);
            Optional.ofNullable(dataSourceClass).map(Class::getName).ifPresent(hikariConfig::setDataSourceClassName);
            apply(username, hikariConfig::setUsername);
            apply(password, hikariConfig::setPassword);
            apply(minPoolSize, hikariConfig::setMinimumIdle);
            apply(maxPoolSize, hikariConfig::setMaximumPoolSize);
            return hikariConfig;
        }

        private <T> void apply(T value, Consumer<T> consumer) {
            Optional.ofNullable(value).ifPresent(consumer);
        }

    }
}

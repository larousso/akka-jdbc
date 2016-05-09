package com.adelegue.akka.jdbc;

import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.adelegue.akka.jdbc.DbUtils.Superhero;
import com.zaxxer.hikari.HikariConfig;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by adelegue on 08/05/2016.
 */
public class DatabaseBuilderTest {

    @Test
    public void testBuilder() throws ExecutionException, InterruptedException, ClassNotFoundException {

        Pair<DataSource, String> datasetAndGetDatasource = DbUtils.createDatasetAndGetDatasource();
        Database db = Database.builder()
                .withUrl(datasetAndGetDatasource.second())
                .withThreadPoolSize(2)
                .withMinPoolSize(1)
                .withMaxPoolSize(2)
                .withUsername("user")
                .withPassword("password")
                .build();

        assertThat(db.getActorSystem().settings().config().getInt("akka.jdbc-dispatcher.thread-pool-executor.fixed-pool-size")).isEqualTo(2);

        verify(db);
    }


    @Test
    public void testDatabaseFromHikariConfig() throws ExecutionException, InterruptedException, ClassNotFoundException, SQLException {

        Pair<DataSource, String> datasetAndGetDatasource = DbUtils.createDatasetAndGetDatasource();

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(datasetAndGetDatasource.second());
        hikariConfig.setUsername("user");
        hikariConfig.setPassword("password");
        Database db = Database.from(hikariConfig, "akka.jdbc-dispatcher", 2);
        assertThat(db.getActorSystem().settings().config().getInt("akka.jdbc-dispatcher.thread-pool-executor.fixed-pool-size")).isEqualTo(2);

        verify(db);
    }


    @Test
    public void testDatabaseFromConnection() throws ExecutionException, InterruptedException, ClassNotFoundException, SQLException {

        Pair<DataSource, String> datasetAndGetDatasource = DbUtils.createDatasetAndGetDatasource();

        Connection connection = datasetAndGetDatasource.first().getConnection();
        Database db = Database.from(connection, "akka.jdbc-dispatcher", 2);
        assertThat(db.getActorSystem().settings().config().getInt("akka.jdbc-dispatcher.thread-pool-executor.fixed-pool-size")).isEqualTo(2);

        verify(db);
    }

    @Test
    public void testDatabaseFromDatasource() throws ExecutionException, InterruptedException, ClassNotFoundException, SQLException {

        Pair<DataSource, String> datasetAndGetDatasource = DbUtils.createDatasetAndGetDatasource();

        Database db = Database.from(datasetAndGetDatasource.first(), "akka.jdbc-dispatcher", 2);
        assertThat(db.getActorSystem().settings().config().getInt("akka.jdbc-dispatcher.thread-pool-executor.fixed-pool-size")).isEqualTo(2);

        verify(db);
    }



    private void verify(Database db) throws ExecutionException, InterruptedException {
        ActorMaterializer materializer = ActorMaterializer.create(db.getActorSystem());

        Source<Superhero, ?> source = db.sql()
                .select("select * from superhero where city_id = ?")
                .param(3)
                .as(Superhero::convert)
                .get();

        List<Superhero> superheros = source.runWith(Sink.seq(), materializer).toCompletableFuture().get();
        assertThat(superheros).contains(new Superhero(1, "hitachi", 30, 3), new Superhero(2, "naruto", 30, 3));
    }


}

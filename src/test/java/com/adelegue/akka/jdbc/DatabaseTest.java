package com.adelegue.akka.jdbc;


import akka.japi.Pair;
import akka.japi.tuple.Tuple4;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.adelegue.akka.jdbc.DbUtils.City;
import com.adelegue.akka.jdbc.DbUtils.Superhero;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import org.h2.jdbc.JdbcSQLException;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.adelegue.akka.jdbc.Sql.andThen;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test Database api.
 *
 * Created by adelegue on 01/05/2016.
 */
public class DatabaseTest {

    @Test
    public void simpleSelect() throws Exception {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql();

        Source<Superhero, ?> source = sql
                .select("select * from superhero where city_id = ?")
                .param(3)
                .as(Superhero::convert)
                .get();

        List<Superhero> superheros = source.runWith(Sink.seq(), materializer).toCompletableFuture().get();

        assertThat(superheros).contains(new Superhero(1, "hitachi", 30, 3), new Superhero(2, "naruto", 30, 3));
        assertThat(await(sql.getConnection()).connection().isClosed()).isEqualTo(Boolean.TRUE);
    }


    @Test
    public void simple_select_to_map() throws Exception {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());

        Source<Map<String, Object>, ?> source = database.sql()
                .select("select * from superhero where city_id = ?")
                .param(3).get(Convertions::map);

        List<Map<String, Object>> superheros = source.runWith(Sink.seq(), materializer).toCompletableFuture().get();

        assertThat(superheros).isEqualTo(Arrays.asList(
                new HashMap<String, Object>() {{
                    put("ID", 3L);
                    put("NAME", "hitachi");
                    put("PUISSANCE", 30);
                    put("CITY_ID", 3);
                }},
                new HashMap<String, Object>() {{
                    put("ID", 4L);
                    put("NAME", "naruto");
                    put("PUISSANCE", 30);
                    put("CITY_ID", 3);
                }}
        ));
    }

    @Test
    public void simple_select_to_tuple() throws Exception {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());

        Source<Tuple4<Long, String, Integer, Integer>, ?> source = database.sql()
                .select("select * from superhero where city_id = ?")
                .param(3).get(Convertions.as(Long.class, String.class, Integer.class, Integer.class));

        List<Tuple4<Long, String, Integer, Integer>> superheros = source.runWith(Sink.seq(), materializer).toCompletableFuture().get();

        assertThat(superheros).isEqualTo(Arrays.asList(
            Tuple4.create(3L, "hitachi", 30, 3),
            Tuple4.create(4L, "naruto", 30, 3)
        ));
    }

    static <T> T await(Future<T> f) throws Exception {
        return Await.result(f, Duration.apply(1, TimeUnit.SECONDS));
    }

    @Test
    public void select_multiple_connection() throws Exception {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql();
        Source<Superhero, ?> source = sql
                .select("select * from superhero where city_id = ?")
                .param(3)
                .get(Superhero::convert);
        Sql sql2 = database.sql();
        Source<Superhero, ?> source2 = sql2
                .select("select * from superhero where city_id = ?")
                .param(3)
                .get(Superhero::convert);

        List<Superhero> superheros = source.runWith(Sink.seq(), materializer).toCompletableFuture().get();
        List<Superhero> superheros2 = source2.runWith(Sink.seq(), materializer).toCompletableFuture().get();

        assertThat(superheros).isEqualTo(superheros2);
        assertThat(await(sql.getConnection()).name())
                .isNotEqualTo(await(sql2.getConnection()).name());
        assertThat(await(sql.getConnection()).connection().isClosed()).isEqualTo(Boolean.TRUE);
        assertThat(await(sql2.getConnection()).connection().isClosed()).isEqualTo(Boolean.TRUE);
    }

    @Test
    public void select_leaving_connection_opened() throws Exception {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql();
        Source<Superhero, ?> source = sql.keepConnectionOpened()
                .select("select * from superhero where city_id = ?")
                .param(3)
                .get(Superhero::convert);

        List<Superhero> superheros = source.runWith(Sink.seq(), materializer).toCompletableFuture().get();

        assertThat(superheros).contains(new Superhero(1, "hitachi", 30, 3), new Superhero(2, "naruto", 30, 3));
        assertThat(await(sql.getConnection()).connection().isClosed()).isEqualTo(Boolean.FALSE);
    }

    @Test
    public void select_leaving_connection_opened_and_closing_with_flow() throws Exception {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql();
        Source<Superhero, ?> source = sql
                .keepConnectionOpened()
                .select("select * from superhero where city_id = ?")
                .param(3)
                .get(Superhero::convert)
                .via(sql.atTheEnd(Sql::closeConnection));

        List<Superhero> superheros = source.runWith(Sink.seq(), materializer).toCompletableFuture().get();

        assertThat(superheros).contains(new Superhero(1, "hitachi", 30, 3), new Superhero(2, "naruto", 30, 3));
        assertThat(await(sql.getConnection()).connection().isClosed()).isEqualTo(Boolean.TRUE);
    }

    @Test
    public void insert_row_and_get_count() throws ExecutionException, InterruptedException {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql();

        Integer update = sql.update("insert into superhero(name, puissance, city_id) values (?, ?, ?) ")
                .params("one punch man", 20, 1)
                .count()
                .runWith(Sink.head(), materializer)
                .toCompletableFuture()
                .get();

        assertThat(update).isEqualTo(1);

        List<Superhero> superheros = database
                .sql()
                .select("select * from superhero where name = ?")
                .param("one punch man")
                .get(Superhero::convert)
                .runWith(Sink.seq(), materializer)
                .toCompletableFuture()
                .get();
        assertThat(superheros.size()).isEqualTo(1);
        assertThat(superheros).contains(new Superhero(4, "one punch man", 20, 1));

    }

    @Test
    public void insert_row_and_get_ids() throws ExecutionException, InterruptedException {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql();

        Integer id = sql.update("insert into superhero(name, puissance, city_id) values (?, ?, ?) ")
                .params("ichigo", 25, 1)
                .returnGeneratedKeys()
                .get(rs -> rs.getInt(1))
                .runWith(Sink.head(), materializer)
                .toCompletableFuture()
                .get();

        List<Superhero> superheros = database
                .sql()
                .select("select * from superhero where id = ?")
                .param(id)
                .as(Superhero.as())
                .get()
                .runWith(Sink.seq(), materializer)
                .toCompletableFuture()
                .get();
        assertThat(superheros.size()).isEqualTo(1);
        assertThat(superheros).contains(new Superhero(id, "ichigo", 25, 1));

    }

    @Test
    public void insert_row_with_error() throws Exception {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql();

        assertThatThrownBy(() ->
                sql.update("insert into superhero(name, puissance, city_id) values (?, ?, ?) ")
                        .params("naruto", 20, 1)
                        .count()
                        .runWith(Sink.head(), materializer)
                        .toCompletableFuture()
                        .get()
        ).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(JdbcSQLException.class);

        assertThat(await(sql.getConnection()).connection().isClosed()).isEqualTo(true);
    }

    @Test
    public void select_depending_on_insert() throws ExecutionException, InterruptedException {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql();

        Source<Integer, ?> update = sql.update("insert into superhero(name, puissance, city_id) values (?, ?, ?) ")
                .params("one punch man", 20, 1)
                .count();

        List<Superhero> superheros = database
                .sql()
                .select("select * from superhero where name = ?")
                .param("one punch man")
                .dependsOn(update)
                .get(Superhero::convert)
                .runWith(Sink.seq(), materializer)
                .toCompletableFuture()
                .get();
        assertThat(superheros.size()).isEqualTo(1);
        assertThat(superheros).contains(new Superhero(4, "one punch man", 20, 1));
    }


    @Test
    public void insert_with_in_params() throws ExecutionException, InterruptedException {
        Pair<DataSource, String> datasetAndGetDatasource = DbUtils.createDatasetAndGetDatasource();
        Database database = Database.builder()
                .withUrl(datasetAndGetDatasource.second())
                .withUsername("user")
                .withPassword("password")
                .build();
        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());

        List<Integer> resultSets = Source.range(4, 53)
                .map(i -> Pair.create(i, "name" + i))
                .via(Sql.andThen( pair ->
                    database
                            .sql()
                            .update("insert into city(city_id, name) values (?, ?)")
                            .params(pair.first(), pair.second())
                            .count()
                ))
                .runWith(Sink.seq(), materializer)
                .toCompletableFuture()
                .get();

        Integer sum = resultSets.stream().reduce(0, (acc, elt) -> acc + elt);
        assertThat(sum).isEqualTo(50);
    }

    @Test
    public void select_depending_on_insert_with_error() throws Exception {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql();

        Source<Integer, ?> update = sql.update("insert into superhero(name, puissance, city_id) values (?, ?, ?) ")
                .params("naruto", 20, 1)
                .count();
        assertThatThrownBy(() -> database
                .sql()
                .select("select * from superhero where name = ?")
                .param("one punch man")
                .dependsOn(update)
                .as(Superhero::convert)
                .get()
                .runWith(Sink.seq(), materializer)
                .toCompletableFuture()
                .get()
        ).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(JdbcSQLException.class);
        assertThat(await(sql.getConnection()).connection().isClosed()).isEqualTo(true);
    }

    @Test
    public void select_chaining_with_select() throws Exception {
        Database database = DbUtils.pooledDb();

        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());
        Sql sql = database.sql().keepConnectionOpened();

        Source<City, ?> cities = sql
                .select("select * from superhero")
                .as(Superhero::convert)
                .get()
                .via(sql.select("select * from city where city_id = ? ")
                        .as(City::convert)
                        .<Superhero>grabInParam(s -> s.city_id)
                )
                .via(sql.atTheEnd(Sql::closeConnection));

        List<City> cityList = cities.runWith(Sink.seq(), materializer).toCompletableFuture().get();
        List<City> expected = Arrays.asList(City.of(1, "New york"), City.of(2, "Tokyo"), City.of(3, "Konoha"), City.of(3, "Konoha"));
        assertThat(cityList).contains(City.of(1, "New york"), City.of(2, "Tokyo"), City.of(3, "Konoha"));
        assertThat(cityList).isEqualTo(expected);
        assertThat(await(sql.getConnection()).connection().isClosed()).isEqualTo(true);
    }

    @Test
    public void get_connection() throws ExecutionException, InterruptedException {
        Database database = DbUtils.pooledDb();
        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());

        List<SqlConnection> sqlConnection = database.sql().connection().runWith(Sink.seq(), materializer).toCompletableFuture().get();
        System.out.println(sqlConnection);
        assertThat(sqlConnection.size()).isEqualTo(1);
        assertThat(sqlConnection.get(0).name()).isEqualTo("AkkaJdbcConnection0");
    }

    @Test
    public void update_on_transaction() throws Exception {

        Database database = DbUtils.pooledDb();
        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());

        Sql sql = database.sql().keepConnectionOpened();

        sql
                .beginTransaction()
                .via(andThen(any ->
                        sql.update("insert into superhero(name, puissance, city_id) values (?, ?, ?) ")
                        .params("one punch man", 20, 1)
                        .count()
                ))
                .via(sql.doOnEachWithInParam(
                        Sql.doAndCommit(i -> System.out.println("Generated id = " + i)))
                )
                .via(sql.atTheEnd()
                        .then(Sql.endTransaction()).and(Sql.closeConnection())
                        .apply()
                )
                .runWith(Sink.head(), materializer)
                .toCompletableFuture()
                .get();

        List<Superhero> superheros = database.sql()
                .select("select * from superhero where name = ?")
                .param("one punch man")
                .get(Superhero::convert)
                .runWith(Sink.seq(), materializer)
                .toCompletableFuture()
                .get();
        assertThat(superheros.size()).isEqualTo(1);
        assertThat(superheros).contains(new Superhero(1, "one punch man", 20, 1));
        assertThat(await(sql.getConnection()).connection().isClosed()).isEqualTo(true);
    }

    @Test
    public void update_and_rollback() throws Exception {

        Database database = DbUtils.pooledDb();
        ActorMaterializer materializer = ActorMaterializer.create(database.getActorSystem());

        Sql sql = database.sql().keepConnectionOpened();

        sql
                .beginTransaction()
                .via(andThen(any ->
                    sql.update("insert into superhero(name, puissance, city_id) values (?, ?, ?) ")
                        .params("one punch man", 20, 1)
                        .andRollback()
                        .count()
                ))
                .via(sql.atTheEnd(Sql::closeConnection))
                .runWith(Sink.head(), materializer)
                .toCompletableFuture()
                .get();

        List<Superhero> superheros = database.sql()
                .select("select * from superhero where name = ?")
                .param("one punch man")
                .get(Superhero::convert)
                .runWith(Sink.seq(), materializer)
                .toCompletableFuture()
                .get();
        assertThat(superheros.size()).isEqualTo(0);
        assertThat(await(sql.getConnection()).connection().isClosed()).isEqualTo(true);
    }



}
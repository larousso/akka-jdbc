package com.adelegue.akka.jdbc;

import akka.japi.Pair;
import com.adelegue.akka.jdbc.exceptions.SqlException;
import com.adelegue.akka.jdbc.exceptions.ExceptionsHandler;
import com.adelegue.akka.jdbc.function.ResultSetExtractor;
import org.h2.jdbcx.JdbcConnectionPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by adelegue on 01/05/2016.
 */
public class DbUtils {

    private static AtomicInteger dbNumber = new AtomicInteger();

    static String nextDbUrl() {
        return "jdbc:h2:mem:test" + dbNumber.getAndIncrement() + ";DB_CLOSE_DELAY=-1";
    }

    public static Database pooledDb() {
        return Database.from(createDatasetAndGetDatasource().first());
    }

    public static Database oneConnectionDb() {
        DataSource ds = JdbcConnectionPool.create(nextDbUrl(), "user", "password");
        Connection connection = createDatasetFromDataSource(ds);
        return Database.from(connection);
    }


    public static Pair<DataSource, String> createDatasetAndGetDatasource() {
        String url = nextDbUrl();
        DataSource ds = JdbcConnectionPool.create(url, "user", "password");
        createDatasetFromDataSource(ds);
        return Pair.create(ds, url);
    }

    private static Connection createDatasetFromDataSource(DataSource ds) {
        Connection connection = ExceptionsHandler.handleChecked(ds::getConnection);
        createDatabase(connection);
        ExceptionsHandler.handleChecked0(connection::close);
        return connection;
    }


    public static void createDatabase(Connection c) {
        try {
            c.setAutoCommit(true);
            c.prepareStatement(
                    "create table city (city_id int primary key, name varchar(100) not null)")
                    .execute();
            c.prepareStatement(
                    "insert into city(city_id, name) values(1,'New york')")
                    .execute();
            c.prepareStatement(
                    "insert into city(city_id, name) values(2,'Tokyo')")
                    .execute();
            c.prepareStatement(
                    "insert into city(city_id, name) values(3,'Konoha')")
                    .execute();
            c.prepareStatement(
                    "create table superhero (id bigint auto_increment primary key, name varchar(50) unique, puissance int not null, city_id int)")
                    .execute();
            c.prepareStatement("insert into superhero(name,puissance,city_id) values('spiderman',10, 1)").execute();
            c.prepareStatement("insert into superhero(name,puissance,city_id) values('sangoku',50, 2)").execute();
            c.prepareStatement("insert into superhero(name,puissance,city_id) values('hitachi',30, 3)").execute();
            c.prepareStatement("insert into superhero(name,puissance,city_id) values('naruto',30, 3)").execute();

        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }



    public static class City {
        final Integer id;
        final String name;

        public City(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        public static City of(Integer id, String name) {
            return new City(id, name);
        }

        public static City convert(ResultSet rs) throws SQLException {
            return new City(rs.getInt(1), rs.getString(2));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            City city = (City) o;
            return Objects.equals(id, city.id) &&
                    Objects.equals(name, city.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }

        @Override
        public String toString() {
            return "City{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static class Superhero {
        final Integer id;
        final String name;
        final Integer puissance;
        final Integer city_id;

        public Superhero(Integer id, String name, Integer puissance, Integer city_id) {
            this.id = id;
            this.name = name;
            this.puissance = puissance;
            this.city_id = city_id;
        }

        static Superhero convert(ResultSet rs) throws SQLException {
            return new Superhero(rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getInt(4));
        }

        static ResultSetExtractor<Superhero> as() {
            return rs -> new Superhero(rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getInt(4));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Superhero) {
                Superhero other = Superhero.class.cast(obj);
                return Objects.equals(name, other.name);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return "Superhero{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", puissance=" + puissance +
                    ", city_id=" + city_id +
                    '}';
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }
    }
}

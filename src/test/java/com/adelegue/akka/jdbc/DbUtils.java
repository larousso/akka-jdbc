package com.adelegue.akka.jdbc;

import com.adelegue.akka.jdbc.exceptions.SqlException;
import com.adelegue.akka.jdbc.exceptions.ExceptionsHandler;
import org.h2.jdbcx.JdbcConnectionPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
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
        DataSource ds = JdbcConnectionPool.create(nextDbUrl(), "user", "password");
        createTables(ds);
        return Database.from(ds);
    }

    public static Database oneConnectionDb() {
        DataSource ds = JdbcConnectionPool.create(nextDbUrl(), "user", "password");
        Connection connection = createTables(ds);
        return Database.from(connection);
    }

    private static Connection createTables(DataSource ds) {
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
}

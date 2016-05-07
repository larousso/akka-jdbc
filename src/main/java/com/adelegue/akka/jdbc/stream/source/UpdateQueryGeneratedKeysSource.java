package com.adelegue.akka.jdbc.stream.source;

import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import com.adelegue.akka.jdbc.query.Query;
import com.adelegue.akka.jdbc.query.SqlContext;
import com.adelegue.akka.jdbc.query.Transaction;
import com.adelegue.akka.jdbc.stream.ResourcesHelper;
import com.adelegue.akka.jdbc.utils.ResultSetExtractor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

/**
 * Created by adelegue on 30/04/2016.
 */
public class UpdateQueryGeneratedKeysSource<T> extends GraphStage<SourceShape<T>> implements ResourcesHelper {


    private final SqlContext sqlContext;

    private final Query query;

    private final ResultSetExtractor<T> rsConverter;

    private final Optional<Transaction> transaction;

    // Define the (sole) output port of this stage
    private final Outlet<T> out = Outlet.create("ResultSet.out");

    // Define the shape of this stage, which is SourceShape with the port we defined above
    private final SourceShape<T> shape = SourceShape.of(out);

    public UpdateQueryGeneratedKeysSource(SqlContext sqlContext, Query query, ResultSetExtractor<T> rsConverter, Optional<Transaction> transaction) {
        this.sqlContext = sqlContext;
        this.query = query;
        this.rsConverter = rsConverter;
        this.transaction= transaction;
    }

    @Override
    public SourceShape<T> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {

            final LoggingAdapter log = Logging.getLogger(sqlContext.actorSystem, this);

            private Boolean executed = false;
            private ResultSet resultSet;
            private Statement statement;
            {
                setHandler(out, new AbstractOutHandler() {

                    @Override
                    public void onDownstreamFinish() throws Exception {
                        cleanResources(statement, sqlContext.connection, transaction);
                    }

                    @Override
                    public void onPull() throws Exception {
                        try {
                            if (!executed) {
                                log.debug("Preparing statement for update query {} on connection {}", query, sqlContext.connection.name());
                                PreparedStatement preparedStatement = query.buildPreparedStatement(sqlContext.connection);
                                statement = preparedStatement;
                                log.debug("Executing update query {} on connection {}", query, sqlContext.connection.name());
                                preparedStatement.executeUpdate();
                                executed = true;
                                resultSet = preparedStatement.getGeneratedKeys();
                            }
                            boolean next = resultSet.next();
                            if (next) {
                                push(out, rsConverter.get(resultSet));
                            } else {
                                log.debug("Closing statement for query {} on connection {}", query, sqlContext.connection.name());
                                cleanResources(statement, sqlContext.connection, transaction);
                                complete(out);
                            }
                        } catch (SQLException e) {
                            cleanResources(statement, sqlContext.connection, transaction, Boolean.TRUE);
                            fail(out, e);
                            throw e;
                        }
                    }
                });
            }
        };
    }
}

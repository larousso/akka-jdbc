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
import com.adelegue.akka.jdbc.stream.ResourcesHelper;
import com.adelegue.akka.jdbc.query.SqlContext;
import com.adelegue.akka.jdbc.query.Transaction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

/**
 * Created by adelegue on 30/04/2016.
 */
public class UpdateQuerySource extends GraphStage<SourceShape<Integer>> implements ResourcesHelper {


    private final SqlContext sqlContext;

    private final Query query;

    private final Optional<Transaction> transaction;

    // Define the (sole) output port of this stage
    private final Outlet<Integer> out = Outlet.create("ResultSet.out");

    // Define the shape of this stage, which is SourceShape with the port we defined above
    private final SourceShape<Integer> shape = SourceShape.of(out);

    public UpdateQuerySource(SqlContext sqlContext, Query query, Optional<Transaction> transaction) {
        this.sqlContext = sqlContext;
        this.query = query;
        this.transaction = transaction;
    }

    @Override
    public SourceShape<Integer> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {

            final LoggingAdapter log = Logging.getLogger(sqlContext.actorSystem, this);

            private Statement statement;
            private int result;
            {
                setHandler(out, new AbstractOutHandler() {

                    private Boolean executed = false;

                    @Override
                    public void onDownstreamFinish() throws Exception {
                        cleanResources(statement, sqlContext.connection, transaction);
                    }

                    @Override
                    public void onPull() throws Exception {
                        if(!executed) {
                            try {
                                log.debug("Preparing statement for update query {} on connection {}", query, sqlContext.connection.name());
                                PreparedStatement preparedStatement = query.buildPreparedStatement(sqlContext.connection);
                                statement = preparedStatement;
                                log.debug("Executing update query {} on connection {}", query, sqlContext.connection.name());
                                result = preparedStatement.executeUpdate();
                                executed = true;
                                push(out, result);
                                cleanResources(statement, sqlContext.connection, transaction);
                                complete(out);
                            } catch (SQLException e) {
                                cleanResources(statement, sqlContext.connection, transaction, Boolean.TRUE);
                                throw e;
                            }
                        } else {
                            complete(out);
                        }
                    }
                });
            }
        };
    }
}

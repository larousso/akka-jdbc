package com.adelegue.akka.jdbc.stream.stage;

import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import com.adelegue.akka.jdbc.query.Query;
import com.adelegue.akka.jdbc.query.SqlContext;
import com.adelegue.akka.jdbc.stream.ResourcesHelper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

/**
 * Created by adelegue on 30/04/2016.
 */
public class UpdateQueryStage extends GraphStage<FlowShape<SqlContext, Integer>> implements ResourcesHelper {

    private final Query query;

    private final Inlet<SqlContext> in = Inlet.create("UpdateQueryStage.in");
    private final Outlet<Integer> out = Outlet.create("UpdateQueryStage.out");
    private final FlowShape<SqlContext, Integer> shape = FlowShape.of(in, out);

    public UpdateQueryStage(Query query) {
        this.query = query;
    }

    @Override
    public FlowShape<SqlContext, Integer> shape() {
        return shape;
    }
    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {

            private Statement statement;
            private int result;
            private Boolean executed = false;
            {
                setHandler(in, new AbstractInHandler() {

                    @Override
                    public void onPush() throws Exception {
                        SqlContext sqlContext = grab(in);
                        final LoggingAdapter log = Logging.getLogger(sqlContext.actorSystem, this);
                        try {
                            log.debug("Preparing statement for update query {} on connection {}", query, sqlContext.connection.name());
                            PreparedStatement preparedStatement = query.buildPreparedStatement(sqlContext.connection);
                            statement = preparedStatement;
                            log.debug("Executing update query {} on connection {}", query, sqlContext.connection.name());
                            result = preparedStatement.executeUpdate();
                            executed = true;
                            push(out, result);
                            cleanResources(statement, sqlContext.connection, Optional.empty());
                            complete(out);
                        } catch (SQLException e) {
                            cleanResources(statement, sqlContext.connection, Optional.empty());
                            fail(out, e);
                        }
                    }
                });
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        pull(in);
                    }
                });
            }
        };
    }
}

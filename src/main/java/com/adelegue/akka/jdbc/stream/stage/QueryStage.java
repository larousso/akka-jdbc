package com.adelegue.akka.jdbc.stream.stage;

import akka.japi.Option;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import com.adelegue.akka.jdbc.query.Query;
import com.adelegue.akka.jdbc.stream.ResourcesHelper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;
import java.util.function.Function;

/**
 * Created by adelegue on 30/04/2016.
 */
public class QueryStage<T> extends GraphStage<FlowShape<SqlConnection, T>> implements ResourcesHelper {

    final Inlet<SqlConnection> in = Inlet.create("QueryStage.in");
    final Outlet<T> out = Outlet.create("QueryStage.out");

    private final Query query;
    private final Function<ResultSet, T> rsConverter;

    public QueryStage(Query query, Function<ResultSet, T> rsConverter) {
        this.query = query;
        this.rsConverter = rsConverter;
    }

    private final FlowShape<SqlConnection, T> shape = FlowShape.of(in, out);
    @Override
    public FlowShape<SqlConnection, T> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape) {
            private Boolean executed = false;
            private Option<ResultSet> resultSet;
            private Statement statement;
            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        SqlConnection connection = grab(in);
                        PreparedStatement preparedStatement = query.buildPreparedStatement(connection);
                        statement = preparedStatement;
                        ResultSet rs = preparedStatement.executeQuery();
                        resultSet = Option.some(rs);
                        boolean next = rs.next();
                        if (next) {
                            push(out, rsConverter.apply(rs));
                        } else {
                            //clean ressources
                            cleanResources(statement, connection, Optional.empty());
                            pull(in);
                        }
                    }
                });
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        if(resultSet.isDefined()) {
                            ResultSet rs = resultSet.get();
                            boolean next = rs.next();
                            if (next) {
                                push(out, rsConverter.apply(rs));
                            } else {
                                pull(in);
                            }
                        } else {
                            pull(in);
                        }
                    }
                });
            }
        };
    }
}

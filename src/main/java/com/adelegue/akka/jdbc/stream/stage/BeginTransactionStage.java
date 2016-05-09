package com.adelegue.akka.jdbc.stream.stage;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import com.adelegue.akka.jdbc.connection.SqlConnection;

/**
 * Created by adelegue on 30/04/2016.
 */
public class BeginTransactionStage extends GraphStage<FlowShape<SqlConnection, SqlConnection>> {

    final Inlet<SqlConnection> in = Inlet.create("BeginTransactionStage.in");
    final Outlet<SqlConnection> out = Outlet.create("BeginTransactionStage.out");

    private final FlowShape<SqlConnection, SqlConnection> shape = FlowShape.of(in, out);
    @Override
    public FlowShape<SqlConnection, SqlConnection> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape) {
            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        SqlConnection connection = grab(in);
                        if(connection != null) {
                            connection.connection().setAutoCommit(false);
                            push(out, connection);
                        }
                        complete(out);
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

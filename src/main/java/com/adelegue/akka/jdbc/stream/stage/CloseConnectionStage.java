package com.adelegue.akka.jdbc.stream.stage;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import com.adelegue.akka.jdbc.stream.ResourcesHelper;
import scala.compat.java8.FutureConverters;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.concurrent.CompletionStage;

/**
 * Created by adelegue on 30/04/2016.
 */
public class CloseConnectionStage<T> extends GraphStage<FlowShape<T, T>> implements ResourcesHelper {

    final Inlet<T> in = Inlet.create("QueryStage.in");
    final Outlet<T> out = Outlet.create("QueryStage.out");

    private final CompletionStage<SqlConnection> sqlConnection;

    private final ExecutionContext executionContext;

    public CloseConnectionStage(Future<SqlConnection> sqlConnection, ExecutionContext executionContext) {
        this.sqlConnection = FutureConverters.toJava(sqlConnection);
        this.executionContext = executionContext;
    }

    private final FlowShape<T, T> shape = FlowShape.of(in, out);
    @Override
    public FlowShape<T, T> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape) {

            Boolean first = true;
            SqlConnection currentConnection;

            AsyncCallback<SqlConnection> setConnectionAndPush = createAsyncCallback(ctn -> {
                pull(in);
                currentConnection = ctn;
                first = false;
            });
            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        push(out, grab(in));
                    }

                    @Override
                    public void onUpstreamFinish() throws Exception {
                        if(currentConnection != null) {
                            currentConnection.connection().close();
                        }
                        complete(out);
                    }

                    @Override
                    public void onUpstreamFailure(Throwable ex) throws Exception {
                        if(currentConnection != null) {
                            currentConnection.connection().close();
                        }
                        fail(out, ex);
                    }
                });

                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onDownstreamFinish() throws Exception {
                        //Stop propagation of the downstream finish event ???
//                        if(currentConnection != null) {
//                            currentConnection.connection().close();
//                        }
                    }

                    @Override
                    public void onPull() throws Exception {
                        if(first) {
                            sqlConnection.thenAccept(setConnectionAndPush::invoke);
                        } else {
                            pull(in);
                        }
                    }
                });
            }
        };
    }
}

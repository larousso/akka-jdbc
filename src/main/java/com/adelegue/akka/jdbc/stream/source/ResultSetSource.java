package com.adelegue.akka.jdbc.stream.source;

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;

import java.sql.ResultSet;

/**
 * Created by adelegue on 29/04/2016.
 */

public class ResultSetSource extends GraphStage<SourceShape<ResultSet>> {
    // Define the (sole) output port of this stage
    public final Outlet<ResultSet> out = Outlet.create("ResultSet.out");

    // Define the shape of this stage, which is SourceShape with the port we defined above
    private final SourceShape<ResultSet> shape = SourceShape.of(out);

    private final ResultSet resultSet;

    public ResultSetSource(ResultSet resultSet) {
        super();
        this.resultSet = resultSet;
    }

    @Override
    public SourceShape<ResultSet> shape() {
        return shape;
    }

    // This is where the actual (possibly stateful) logic is created
    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {
            {
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        boolean next = resultSet.next();
                        if (next) {
                            push(out, resultSet);
                        } else {
                            complete(out);
                        }
                    }


                });
            }
        };
    }

}

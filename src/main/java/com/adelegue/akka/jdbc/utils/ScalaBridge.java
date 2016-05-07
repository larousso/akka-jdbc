package com.adelegue.akka.jdbc.utils;

import scala.Function1;
import scala.runtime.AbstractFunction1;

import java.util.function.Function;

/**
 * Created by adelegue on 02/05/2016.
 */
public class ScalaBridge {

    public static <In, Out> Function1<In, Out> func(Function<In, Out> func) {
        return new AbstractFunction1<In, Out>() {
            @Override
            public Out apply(In v1) {
                return func.apply(v1);
            }
        };
    }

}

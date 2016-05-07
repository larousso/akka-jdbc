package com.adelegue.akka.jdbc.function;

/**
 * Created by adelegue on 07/05/2016.
 */
public interface F {

    @FunctionalInterface
    interface SupplierUnchecked<T> {
        T execute() throws Exception;
    }

    @FunctionalInterface
    interface Execution {
        void execute() throws Exception;
    }

    @FunctionalInterface
    interface BiConsumerUnchecked<A1, A2> {
        void apply(A1 arg1, A2 arg2) throws Exception;
    }

}

package com.adelegue.akka.jdbc.exceptions;

import com.adelegue.akka.jdbc.function.F.Execution;
import com.adelegue.akka.jdbc.function.F.SupplierUnchecked;

/**
 * Created by adelegue on 29/04/2016.
 */
public class ExceptionsHandler {

    public static <T> T handleChecked(SupplierUnchecked<T> toExecute) {
        try {
            return toExecute.execute();
        } catch (Exception e) {
            throw new SqlException(e);
        }
    }

    public static void handleChecked0(Execution toExecute) {
        try {
            toExecute.execute();
        } catch (Exception e) {
            throw new SqlException(e);
        }
    }

}

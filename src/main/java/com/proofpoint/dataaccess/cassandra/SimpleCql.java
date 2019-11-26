package com.proofpoint.dataaccess.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;

import java.util.concurrent.CompletableFuture;

public interface SimpleCql
{
    String query();
    BoundStatement bind();

    BoundStatement bind0();

    CompletableFuture<ResultSet> batchExecuteAsync(BatchStatement stmt);
}

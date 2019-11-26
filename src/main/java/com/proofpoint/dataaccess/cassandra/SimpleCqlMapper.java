package com.proofpoint.dataaccess.cassandra;

import com.datastax.driver.core.ResultSet;
import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public interface SimpleCqlMapper<T>
{
    /**
     * Execute synchronously.
     *
     * @return DataStax driver's ResultSet for streaming.
     */
    ResultSet execute();

    CompletableFuture<ResultSet> executeAsync();

    default CompletableFuture<Boolean> executeAsyncCheckApplied()
    {
        return executeAsync().thenApply(ResultSet::wasApplied);
    }

    default <R> CompletableFuture<Set<R>> executeAsyncAndMapToColumnSet(Function<T,R> columnMapper)
    {
        return executeAsyncAndMapToColumn(columnMapper, Collectors.toSet());
    }

    default <R> CompletableFuture<List<R>> executeAsyncAndMapToColumnList(Function<T,R> columnMapper)
    {
        return executeAsyncAndMapToColumn(columnMapper, Collectors.toList());
    }


    default <R, C extends Collection<R>> CompletableFuture<C> executeAsyncAndMapToColumn(Function<T,R> columnMapper, Collector<R, ?, C> collector)
    {
        return executeAsyncAndMapToList().thenApply(coll -> coll.stream().map(columnMapper).collect(collector));
    }

    default <R> CompletableFuture<Optional<R>> executeAsyncAndMapOneOptionalColumn(Function<T,R> columnMapper) {
        return executeAsyncAndMapAtMostOne().thenApply(opt-> opt.map(columnMapper));
    }

    CompletableFuture<List<T>> executeAsyncAndMapToList();

    @SuppressWarnings("unchecked")
    default CompletableFuture<Collection<T>> executeAsyncAndMap()
    {
        return (CompletableFuture<Collection<T>>)(CompletableFuture<?>)executeAsyncAndMapToList();
    }

    CompletableFuture<Optional<T>> executeAsyncAndMapOne();

    CompletableFuture<Optional<T>> executeAsyncAndMapAtMostOne();

//    Object[] getParamValues();

    default Class<? super T> getTypeParameterClass()
    {
        return new TypeToken<T>(getClass())
        {
        }.getRawType();
    }
}

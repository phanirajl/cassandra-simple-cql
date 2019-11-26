package com.proofpoint.dataaccess.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import javax.annotation.Nullable;
import javax.persistence.Column;
import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Why this API?
 * Datastax' Cassandra driver has 3 APIs: the basic, QueryBuilder (helps get CQL if you can't by yourself), and get mapper.
 * So QueryBuilder is helpful in one thing: obscuring the actual CQL statement. Not using it.
 * Object mapper sounds nice but has a bunch of shortcomings:<UL>
 * <LI>hard to define custom get, for example using now() for timeuuid. Require defining Accessor which is not as nice as mapped get.</LI>
 * <LI>hard to do LWT (using conditional IF) - requires defining Accessor.</LI>
 * <LI>hard to do unit testing via Scassandra. The mapper requires the schema to be defined in Cassandra, but with simulated cassandra it would require a lot of priming queries for system tables. Makes testing code very brittle.</LI>
 * <LI>hard (but possible) to do batch queries.</LI>
 * <LI>Cassandra-level objects that are modeled after the Cassandra schema are not useful for at application abstraction level. Remapping to/from application-level objects is required.</LI>
 * </UL>
 * So what is left is the basic API. The problem with basic API is that get is usually defined as a string constant in one place, but used (prepare, bind parameters) in another.
 * This creates human error trap inviting to mismatch the params/columns with respect to the actual get.
 * <p>
 * So SimpleCqlMapper addresses this problem by letting you to: <UL>
 * <LI>define get and its accessor in a single place. This is similar to get mapper but we define the get. This is what Accessor is doing too, but...</LI>
 * <LI>easily write unit tests via Scassandra, and automate all the plumbing code.</LI>
 * </UL>
 *
 * @param <T> your simple query definition class
 */
public final class SimpleCqlFactory<T extends SimpleCqlMapper> implements SimpleCqlProvider<T>
{
    private static final int MAX_N_TABLES = 400;

    public interface ByNameAccessor
    {
        Object apply(Row row, String t);
    }

    static final class GetterHelper
    {
        final String name;
        final Method method;
        final ByNameAccessor accessor;
        final boolean ignored;
        final boolean custom;
        final boolean sensitive;
        final boolean ignore_null;

        GetterHelper(Method m, ByNameAccessor accessor)
        {
            this.name = mapName(m);
            this.method = m;
            this.accessor = accessor;
            SimpleCqlPrettyPrint prettyPrint = m.getDeclaredAnnotation(SimpleCqlPrettyPrint.class);
            ignored = (prettyPrint == null) ? false : prettyPrint.ignore();
            custom = (prettyPrint == null) ? false : prettyPrint.custom();
            sensitive = (prettyPrint == null) ? false : prettyPrint.sensitive();
            ignore_null = (prettyPrint == null) ? true : prettyPrint.ignore_null();
        }
    }

    private final String query;
    protected final Class<? extends SimpleCqlMapper> clazz;
    protected final ArrayList<String> paramNames = new ArrayList<>();

    protected final HashMap<Method, Integer> queryParamBindingMethods = new HashMap<>();
    protected final LinkedHashMap<String, GetterHelper> getterMethods = new LinkedHashMap<>();

    private static final Pattern parameterPattern = Pattern.compile("(\\w+)\\s*=\\s*\\?|:(\\w+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern replaceNamedParametersPattern = Pattern.compile(":\\w+", Pattern.CASE_INSENSITIVE);
    private static final Pattern tidPattern = Pattern.compile("((?:[\\w]+\\.)\\w+)(\\$\\(TID\\))", Pattern.CASE_INSENSITIVE);

    protected CassandraClusterConnector connector;
    protected PreparedStatement[] preparedStatement;
    protected long clockSkew;
    protected int maxTableId;
    private boolean readyToBePrepared = true;
    private Boolean idempotent = null;
    private ConsistencyLevel consistencyLevel = null;
    private ConsistencyLevel serialConsistencyLevel = null;
    private int numRotations = 1;
    private long rotationPeriod;
    private long expirationPeriod;
    private boolean isUpsert = false;
    private boolean isAutoTruncate = false;
    private boolean isSensitivePrettyPrintAllowed = false;
    private final Class<?>[] bindSupers;
    private final Class<?>[] mapperSupers;

    private BiFunction<Object, String, String> prettyPrinter = this::defaultPrettyPrinter;

    static final Constructor<MethodHandles.Lookup> lookupConstructor;

    static {
        // the code below is required to bypass limitation "no private access for invokespecial" when invoking default methods
        Constructor<MethodHandles.Lookup> ctor = null;

        try {
            ctor = Lookup.class.getDeclaredConstructor(Class.class, int.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException("SimpleCqlMapper cannot instantiate MethodHandles.Lookup", e);
        }
        if (!ctor.isAccessible()) {
            ctor.setAccessible(true);
        }

        lookupConstructor = ctor;
    }

    protected SimpleCqlFactory(Class<? extends SimpleCqlMapper> clazz, String query)
    {
        this.clazz = clazz;

        String lowercase = query.toLowerCase();
        if (lowercase.startsWith("update") || lowercase.startsWith("select") || lowercase.startsWith("insert") || lowercase.startsWith("delete")) {
            if (lowercase.startsWith("update") || lowercase.startsWith("insert")) {
                isUpsert = true;
            }

            Matcher m = parameterPattern.matcher(query);
            while (m.find()) {
                String name = MoreObjects.firstNonNull(m.group(1), m.group(2));
                paramNames.add(name);
            }
            query = replaceNamedParametersPattern.matcher(query).replaceAll("?");
        }
        else {
            throw new UnsupportedOperationException("SimpleCqlMapper "+getMapperName()+" cannot parse query: " + query);
        }

        this.query = query;
        try {
            for (int position = 0; position < paramNames.size(); position++) {
                String name = paramNames.get(position).toLowerCase();
                boolean found = false;
                for (Method m : clazz.getMethods()) {
                    if (!m.getName().toLowerCase().equals(name) || (m.getParameterCount() != 1) || m.getDeclaringClass().equals(SimpleCqlMapper.class)) {
                        continue;
                    }
                    queryParamBindingMethods.put(m, position);
                    found = true;
                    break;
                }
                if (!found) {
                    throw new RuntimeException("SimpleCqlMapper "+getMapperName()+" cannot find setter method " + clazz.getSimpleName() + "." + name);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("SimpleCqlMapper "+getMapperName()+" cannot determine retrieve bind index", e);
        }
        Set<Class<?>> binderSuperinterfaces = getSupersRecursively(clazz);
        binderSuperinterfaces.add(SimpleCql.class);
        bindSupers = binderSuperinterfaces.toArray(new Class<?>[binderSuperinterfaces.size()]);

        Class<?> mappedClass = getCqlMapperParameterClass(clazz);
        Set<Class<?>> mapperSuperinterfaces = getSupersRecursively(mappedClass);
        mapperSupers = mapperSuperinterfaces.toArray(new Class<?>[mapperSuperinterfaces.size()]);
        boolean hasDefaultMethods = false;
        for (Method m : mappedClass.getMethods()) {
            if ((m.getParameterCount() != 0) || m.getDeclaringClass().equals(SimpleCqlMapper.class)) {
                continue;
            }
            if (m.isDefault()) {
                hasDefaultMethods = true;
            }
            else {
                getterMethods.put(m.getName(), new GetterHelper(m, getByNameAccessor(m)));
            }
        }
    }

    private static Class<?> getCqlMapperParameterClass(Class<? extends SimpleCqlMapper> cl)
    {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.isDefault()) {
                final Class<?> declaringClass = method.getDeclaringClass();
                Constructor<Lookup> constructor = MethodHandles.Lookup.class.getDeclaredConstructor(Class.class, int.class);
                constructor.setAccessible(true);
                return
                        constructor.newInstance(declaringClass, MethodHandles.Lookup.PRIVATE)
                                .unreflectSpecial(method, declaringClass)
                                .bindTo(proxy)
                                .invokeWithArguments(args);
            }
            throw new RuntimeException("SimpleCqlMapper internal error, invoking " + method);
        };
        try {
            SimpleCqlMapper<?> px = (SimpleCqlMapper<?>) Proxy.newProxyInstance(cl.getClassLoader(), new Class[]{cl, SimpleCqlMapper.class}, handler);
            return px.getTypeParameterClass();

        }
        catch (Exception e) {
            throw new RuntimeException("SimpleCqlMapper internal error in getTypeParameterClass", e);
        }
    }

    static Object getTimeUUID(Row row, String name)
    {
        UUID uuid = row.getUUID(name);
        return (uuid == null) ? null : new TimeUUID(uuid);
    }

    static Object getByteArray(Row row, String name)
    {
        ByteBuffer byteBuffer = row.getBytesUnsafe(name);
        return (byteBuffer == null) ? null : byteBuffer.array();
    }

    static Optional<?> getOptional(Row row, String name)
    {
        Object obj = row.getObject(name);
        return (obj == null)? Optional.empty() : Optional.of(obj);
    }

    ByNameAccessor getByNameAccessor(Method m)
    {
        Class<?> returnType = m.getReturnType();
        if (TimeUUID.class.isAssignableFrom(returnType)) {
            return SimpleCqlFactory::getTimeUUID;
        }
        if (byte[].class.isAssignableFrom(returnType)) {
            return SimpleCqlFactory::getByteArray;
        }
        if (Optional.class.isAssignableFrom(returnType)) {
            return SimpleCqlFactory::getOptional;
        }
        return Row::getObject;
    }

    public List<String> getParamNames()
    {
        return Collections.unmodifiableList(paramNames);
    }

    public List<String> getColumnNames()
    {
        return getterMethods.values().stream().map(getterHelper -> getterHelper.name).collect(Collectors.toList());
    }

    public List<Class<?>> getColumnTypes()
    {
        return getterMethods.values().stream().map(getterHelper -> getterHelper.method.getReturnType()).collect(Collectors.toList());
    }

    public Method getGetterMethod(int index)
    {
        Iterator<GetterHelper> it = getterMethods.values().iterator();
        for (int i = 0; i < getterMethods.size(); i++) {
            GetterHelper g = it.next();
            if (i == index) {
                return g.method;
            }
        }
        throw new IndexOutOfBoundsException("Index too large " + index);
    }

    @VisibleForTesting
    public static String mapName(Method m)
    {
        for (Annotation a : m.getAnnotations()) {
            if (a instanceof Column) {
                return ((Column) a).name();
            }
        }
        if (m.getName().startsWith("get") || m.getName().startsWith("set")) {
            return m.getName().substring(3).toLowerCase();
        }
        return m.getName();
    }


    @VisibleForTesting
    public static Object mapValue(Object value)
    {
        if (value instanceof TimeUUID) {
            return ((TimeUUID) value).getUUID();
        }
        else if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        }
        else {
            return value;
        }
    }

    @VisibleForTesting
    public static Object unmapValue(Object value, Class<?> expectedType)
    {
        if (TimeUUID.class.isAssignableFrom(expectedType)) {
            return new TimeUUID((UUID) value);
        }
        else {
            return value;
        }
    }

    public boolean isReadyToBePrepared()
    {
        return readyToBePrepared;
    }

    @VisibleForTesting
    public void setReadyToBePrepared(boolean b)
    {
        readyToBePrepared = b;
    }

    public static <I extends SimpleCqlMapper> SimpleCqlFactory<I> factory(Class<I> clazz, String query)
    {
        return new SimpleCqlFactory(clazz, query);
    }

    public SimpleCqlFactory<T> prepare(CassandraClusterConnector connector)
    {
        if (!isReadyToBePrepared()) {
            throw new RuntimeException("SimpleCqlMapper "+getMapperName()+" is not ready to prepare query " + query);
        }
        this.connector = connector;
        maxTableId = numRotations - 1;
        preparedStatement = new PreparedStatement[numRotations];

        String tableName = null;
        for (int n = 0; n < numRotations; n++) {
            String q = query;
            if (numRotations > 1) {
                Matcher m = tidPattern.matcher(query);
                StringBuffer sb = new StringBuffer();
                if (m.find()) {
                    tableName = m.group(0);
                    m.appendReplacement(sb, "$1" + n);
                }
                m.appendTail(sb);
                q = sb.toString();
                if (q.equals(query)) {
                    throw new IllegalArgumentException(getMapperName()+": The query must contain $(TID) pattern");
                }
            }
            preparedStatement[n] = connector.getSession().prepare(q);

            if (idempotent != null) {
                preparedStatement[n].setIdempotent(idempotent);
            }
            if (consistencyLevel != null) {
                preparedStatement[n].setConsistencyLevel(consistencyLevel);
            }
            if (serialConsistencyLevel != null) {
                preparedStatement[n].setSerialConsistencyLevel(serialConsistencyLevel);
            }
        }
        if (numRotations > 1) {
            if (isAutoTruncate) {
                connector.scheduleTruncation(tableName, rotationPeriod, numRotations);
            }
            clockSkew = connector.getServerSideTimestampSkew(false);
        }

        return this;
    }

    public SimpleCqlFactory<T> rotations(int nTables)
    {
        if (nTables <= 0) {
            nTables = 1;
        }
        else if (nTables > MAX_N_TABLES) {
            throw new IllegalArgumentException("Too many rotations: " + nTables);
        }
        this.numRotations = nTables;
        return this;
    }

    public long getRotationPeriod()
    {
        return rotationPeriod;
    }

    public long getExpirationPeriod()
    {
        return expirationPeriod;
    }

    public int getNumRotations()
    {
        return numRotations;
    }

    public long getClockSkew()
    {
        return clockSkew;
    }

    public boolean isUpsert()
    {
        return isUpsert;
    }

    public <E> SimpleCqlFactory<T> allowSensitivePrettyPrint(boolean on)
    {
        this.isSensitivePrettyPrintAllowed = on;
        return this;
    }

    public <E> Map<String, Object> asMap(E obj)
    {
        Builder<String, Object> builder = ImmutableMap.builder();

        for (Entry<String, GetterHelper> me : getterMethods.entrySet()) {
            try {
                builder.put(me.getKey(), me.getValue().method.invoke(obj));
            }
            catch (Exception e) {
                throw new IllegalStateException("SimpleCqlMapper "+getMapperName()+": Exception while accessing " + me.getKey(), e);
            }
        }
        return builder.build();
    }

    public SimpleCqlFactory<T> rotationPeriod(long units, TimeUnit timeUnit)
    {
        this.rotationPeriod = timeUnit.toMillis(units);
        return this;
    }

    public SimpleCqlFactory<T> expirationPeriod(long units, TimeUnit timeUnit)
    {
        this.expirationPeriod = timeUnit.toMillis(units);
        return this;
    }

    public SimpleCqlFactory<T> autoTruncate()
    {
        this.isAutoTruncate = true;
        return this;
    }

    public SimpleCqlFactory<T> rotationMs(long ms)
    {
        return rotationPeriod(ms, TimeUnit.MILLISECONDS);
    }

    public SimpleCqlFactory<T> expirationMs(long ms)
    {
        return expirationPeriod(ms, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets whether this statement is idempotent, i.e. whether it can be applied multiple times
     * without changing the result beyond the initial application.
     * <p/>
     * Note: in most cases, Cassandra statements (including upserts) are idempotent.
     * Examples of non-idempotent statements:<UL>
     *     <LI>updating Cassandra counters</LI>
     *     <LI>inserting data where primary key is generated via Cassandra functions like now()</LI>
     * </UL>
     *
     * <p/>
     * See {@link com.datastax.driver.core.Statement#isIdempotent} for more explanations about this property.
     *
     * @param idempotent the new value.
     * @return this {@code IdempotenceAwarePreparedStatement} object.
     */
    public SimpleCqlFactory<T> setIdempotent(boolean idempotent)
    {
        this.idempotent = idempotent;
        return this;
    }

    public SimpleCqlFactory<T> setConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Sets a default serial consistency level. If no serial consistency level is set through this method, the bound statements
     * created from this object will use the default serial consistency level (SERIAL).
     * <p/>
     * See {@link com.datastax.driver.core.PreparedStatement#setSerialConsistencyLevel} for more explanations about this property.
     */
    public SimpleCqlFactory<T> setSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        this.serialConsistencyLevel = consistencyLevel;
        return this;
    }

    public SimpleCqlProvider<T> tid(int tid)
    {
        return new ConfiguredExecution(this).withTableId(tid);
    }

    /**
     * simultaneously query a number rotation tables and return all
     */
    public SimpleCqlProvider<T> faster(int numTables)
    {
        if (isUpsert) {
            throw new UnsupportedOperationException("Multi-table upsert not supported yet");
        }
        return new ConfiguredExecution(this).inParallel(numTables);
    }

    /**
     * sequentially query a number rotation tables and return first found
     */
    public SimpleCqlProvider<T> orderly(int numTables)
    {
        if (isUpsert) {
            throw new UnsupportedOperationException("Multi-table upsert not supported yet");
        }
        return new ConfiguredExecution(this).inSequence(numTables);
    }

    /**
     * try single (and if necessary simultaneously try previous) rotation tables and return all
     */
    public SimpleCqlProvider<T> faster()
    {
        // TODO: support use case when expiration can span rotation. (2 means expiration is smaller than rotation)
        return isUpsert ? new ConfiguredExecution(this).optimalExecution() : new ConfiguredExecution(this).considerExpiration().inParallel(2);
    }

    /**
     * simultaneously try both rotation tables and return all
     */
    public SimpleCqlProvider<T> both()
    {
        // TODO: support use case when expiration can span rotation. (2 means expiration is smaller than rotation)
        return isUpsert ? new ConfiguredExecution(this).optimalExecution() : new ConfiguredExecution(this).inParallel(2);
    }

    /**
     * sequentially try current rotation (and optionally the previous) and return the first item found
     */
    public SimpleCqlProvider<T> orderly()
    {
        // TODO: support use case when expiration can span rotation. (2 means expiration is smaller than rotation)
        return isUpsert ? new ConfiguredExecution(this).optimalExecution() : new ConfiguredExecution(this).considerExpiration().inSequence(2);
    }

    /**
     * try all rotation tables and return the first item found
     */
    public SimpleCqlProvider<T> any()
    {
        // TODO: support use case when expiration can span rotation. (2 means expiration is smaller than rotation)
        return isUpsert ? new ConfiguredExecution(this).optimalExecution() : new ConfiguredExecution(this).inSequence(2);
    }

    public SimpleCqlProvider<T> all()
    {
        // TODO: support use case when expiration can span rotation. (2 means expiration is smaller than rotation)
        return isUpsert ? new ConfiguredExecution(this).optimalExecution() : new ConfiguredExecution(this).optimalExecution(numRotations - 1);
    }

    /**
     * try all rotation tables but prefer the last item found
     */
    public SimpleCqlProvider<T> last()
    {
        // TODO: support use case when expiration can span rotation. (2 means expiration is smaller than rotation)
        return isUpsert ? new ConfiguredExecution(this).optimalExecution() : new ConfiguredExecution(this).pickLast().inSequence(2);
    }

    /**
     * produce new object of the simple query definition class
     *
     * @return instance of simple query definition object
     */
    @Override
    public T get()
    {
        if (connector == null) {
            throw new IllegalStateException("SimpleCqlMapper "+getMapperName()+" has not been prepared");
        }
        return createBinderProxy((ConfiguredExecution<T>) new ConfiguredExecution(this).optimalExecution());
    }

    public static Set<Class<?>> getSupersRecursively(Class<?> clazz)
    {
        Set<Class<?>> res = new HashSet<>();
        res.add(clazz);
        Stream.of(clazz.getInterfaces()).filter(i -> i != SimpleCqlMapper.class).forEach(i -> res.addAll(getSupersRecursively(i)));
        return res;
    }

    private T createBinderProxy(ConfiguredExecution<T> execution)
    {
        try {
            return (T) Proxy.newProxyInstance(clazz.getClassLoader(), bindSupers, new SimpleCqlHandler(execution));
        }
        catch (Exception e) {
            throw new RuntimeException("SimpleCqlMapper "+getMapperName() + " cannot create instance of " + clazz.getName());
        }
    }

    private Object createMapperProxy(final Row row)
    {
        try {
            return Proxy.newProxyInstance(clazz.getClassLoader(), mapperSupers, (proxy, method, args) -> {
                        if (args == null || args.length == 0) {
                            if (method.equals(SimpleCqlHandler.TO_STRING)) {
                                // invoked when application code uses object.toString()
                                return prettyPrintMappedObject(proxy, row, this.prettyPrinter);
                            }
                            if (method.equals(SimpleCqlHandler.HASH_CODE)) {
                                return System.identityHashCode(proxy);
                            }
                            SimpleCqlFactory.GetterHelper columnGetter = getterMethods.get(method.getName());
                            if (columnGetter != null) {
                                return columnGetter.accessor.apply(row, columnGetter.name);
                            }
                            if (method.isDefault()) {
                                final Class<?> declaringClass = method.getDeclaringClass();
                                return lookupConstructor.newInstance(declaringClass, MethodHandles.Lookup.PRIVATE)
                                        .unreflectSpecial(method, declaringClass)
                                        .bindTo(proxy)
                                        .invokeWithArguments(args);
                            }
                        }
                        else {
                            if (method.equals(SimpleCqlHandler.EQUALS)) {
                                return proxy == args[0];
                            }
                            if (method.equals(SimpleCqlHandler.PRETTY_PRINT)) {
                                // invoked when the application code uses object.prettyPrint(formatter-arg)
                                return prettyPrintMappedObject(proxy, row, (BiFunction<Object, String, String>) args[0]);
                            }
                        }
                        throw new RuntimeException("SimpleCqlMapper "+getMapperName()+" cannot invoke method " + method.toString());
                    }
            );
        }
        catch (Exception e) {
            throw new RuntimeException(clazz.getSimpleName() + " SimpleCqlMapper "+getMapperName()+" cannot create instance of " + clazz.getName(), e);
        }
    }

    /**
     * the default pretty printer function simply uses Object#toString
     */
    private String defaultPrettyPrinter(Object obj, String method)
    {
        try {
            return getterMethods.get(method).method.invoke(obj).toString();
        }
        catch (Exception e) {
            return "[" + e.toString() + "]";
        }
    }

    /**
     * allow the user to set custom pretty printer function. The default pretty printer function simply uses Object#toString.
     */
    public <E> SimpleCqlFactory<T> setPrettyPrinter(BiFunction<E, String, String> prettyPrinter)
    {
        this.prettyPrinter = (BiFunction<Object, String, String>) prettyPrinter;
        return this;
    }

    private String prettyPrintMappedObject(Object obj, Row row, BiFunction<Object, String, String> contextPrettyPrinter)
    {
        StringBuilder sb = new StringBuilder();
        for (GetterHelper columnGetter : getterMethods.values()) {
            if (columnGetter.ignored || (columnGetter.sensitive && !isSensitivePrettyPrintAllowed)) {
                continue;
            }

            Object value = (columnGetter.custom) ? contextPrettyPrinter.apply(obj, columnGetter.name) : columnGetter.accessor.apply(row, columnGetter.name);
            String stringValue = MoreObjects.firstNonNull(value, "").toString();
            if ((stringValue == null || stringValue.isEmpty()) && columnGetter.ignore_null) {
                continue;
            }
            sb.append(columnGetter.name)
                    .append('=')
                    .append(stringValue)
                    .append(", ");
        }
        if (sb.length() > 2) {
            sb.delete(sb.length() - 2, sb.length() - 1);
        }
        return sb.toString();
    }

    String getQuery()
    {
        return query;
    }

    String getMapperName()
    {
        return clazz.getSimpleName();
    }

    public PreparedStatement getPreparedStatement()
    {
        return preparedStatement[0];
    }

    public PreparedStatement getPreparedStatement(int tid)
    {
        if (tid < 0 || tid > maxTableId) {
            throw new IllegalArgumentException("Table ID must be between 0-" + maxTableId);
        }
        return preparedStatement[tid];
    }

    public CassandraRotationInfo getRotationInfo()
    {
        return CassandraRotationInfo.getRotationInfo(rotationPeriod, numRotations, clockSkew);
    }

    public Object map(Row row)
    {
        return createMapperProxy(row);
    }

    @Nullable
    public Object mapOne(ResultSet rs)
    {
        if (rs.isExhausted()) {
            return null;
        }
        Row one = rs.one();
        if (!rs.isExhausted()) {
            throw new RuntimeException(clazz.getSimpleName() + ": too many rows");
        }
        return map(one);
    }

    public <M> Optional<M> mapOneOptionally(ResultSet rs)
    {
        Row row = rs.one();
        return (row == null) ? Optional.empty() : Optional.of((M) map(row));
    }

    public <M> List<M> mapCollection(ResultSet rs)
    {
        // TODO: experiment with this code for more optimal async execution
        // below, must use async continuation because there could be blocking I/O calls during iteration
//        return future.thenApply(rs -> {
//            Collection<Object> result = new ArrayList<>();
//            IntStream.range(0, rs.getAvailableWithoutFetching()).forEach(index -> result.add(mapper.map(rs.one())));
//            return new SimpleEntry<>(rs, result);
//        })
//        .thenApplyAsync(pair -> {
//            ResultSet rs = pair.getKey();
//            Collection<Object> result = pair.getValue();
//            // NOTE that this lambda function may issue blocking calls when fetch page is exhausted
//            for (Row row : rs) {
//                result.add(mapper.map(row));
//            }
//            return result;
//        }, executor);

        List<M> result = new ArrayList<>();
        // NOTE that this lambda function may issue blocking calls when fetch page is exhausted
        for (Row row : rs) {
            result.add((M) map(row));
        }
        return result;
    }

    void acquireRequestPermit()
    {
        connector.acquireRequestPermit();
    }

    void releaseRequestPermit()
    {
        connector.releaseRequestPermit();
    }

    public static CompletableFuture<ResultSet> loggedBatchExecAsync(SimpleCqlMapper ... stmts)
    {
        // TODO: implement batch for rotating table, restricting only to insert/update/delete
        BatchStatement batch = new BatchStatement(Type.LOGGED);
        for (SimpleCqlMapper stmt: stmts) {
            batch.add(((SimpleCql)stmt).bind0());
            if (SimpleCqlHandler.logger.isDebugEnabled()) {
                SimpleCqlHandler.logger.debug("Batch \"%s\" query", ((SimpleCql)stmt).query());
            }
        }
        return ((SimpleCql)stmts[0]).batchExecuteAsync(batch);
    }

    public static CompletableFuture<Boolean> loggedBatchExecAsyncCheckApplied(SimpleCqlMapper ... stmts)
    {
        return loggedBatchExecAsync(stmts).thenApply(ResultSet::wasApplied);
    }

    public static class ConfiguredExecution<T1 extends SimpleCqlMapper> implements SimpleCqlProvider<T1>
    {
        private boolean parallel = false;
        private boolean optimal = false;
        private boolean considerExpiration = false;
        private boolean pickLast;
        private int tid = -1;
        private int numTables = -1;
        private final SimpleCqlFactory<T1> factory;

        ConfiguredExecution(SimpleCqlFactory<T1> factory)
        {
            this.factory = factory;
        }

        SimpleCqlFactory<T1> getFactory()
        {
            return factory;
        }

        @Override
        public T1 get()
        {
            return factory.createBinderProxy(this);
        }

        public ConfiguredExecution<T1> withTableId(int tid)
        {
            this.tid = tid;
            return this;
        }

        public ConfiguredExecution<T1> considerExpiration()
        {
            considerExpiration = true;
            return this;
        }

        public ConfiguredExecution<T1> inParallel(int numTables)
        {
            this.numTables = numTables;
            this.parallel = true;
            return this;
        }

        public ConfiguredExecution<T1> inSequence(int numTables)
        {
            this.numTables = numTables;
            return this;
        }

        public ConfiguredExecution<T1> optimalExecution()
        {
            optimal = true;
            considerExpiration = true;
            return this;
        }

        public ConfiguredExecution<T1> optimalExecution(int numTables)
        {
            optimal = true;
            this.numTables = numTables;
            return this;
        }

        public ConfiguredExecution<T1> pickLast()
        {
            pickLast = true;
            return this;
        }

        public boolean isSingleTableExecution()
        {
            return tid != -1;
        }

        public boolean useExpiration()
        {
            return considerExpiration;
        }

        public int getTid()
        {
            return tid;
        }

        public boolean useRangeTid()
        {
            return numTables != -1;
        }

        public boolean isPickLast()
        {
            return pickLast;
        }

        public int getTidRange()
        {
            return numTables;
        }

        public boolean isParallel(boolean mapCollection)
        {
            return parallel || (optimal && mapCollection);
        }

        public boolean isOptimal()
        {
            return optimal;
        }

    }
}

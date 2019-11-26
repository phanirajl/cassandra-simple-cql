package com.proofpoint.dataaccess.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.name.Named;
import com.proofpoint.log.Logger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * TODO: this is a local cache for Cassandra sessions. This should be named CassandraSessionCache and moved to cassandra data-access package
 * i.e. com.proofpoint.appservices.ids.blobstore.cassandra
 */
public class CassandraClusterConnector
{
    public static final int TRUNCATE_TIMEOUT = 3 * 60 * 1000;
    public static final String CONDITIONAL_APPLIED_COLUMN = "[applied]";

    private static final Logger logger = Logger.get(CassandraClusterConnector.class);
    protected final CassandraProperties config;
    private static final Map<String, Session> sessionMap = new ConcurrentHashMap<>();
    protected CompletableFuture<Session> session = new CompletableFuture<>();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final List<Consumer<CassandraClusterConnector>> preparers = new ArrayList<>();
    private final List<Consumer<Cluster>> clusterConfigurers = new ArrayList<>();
    private final String name;
    private volatile long clockSkew;
    private Semaphore permits;
    private static final Map<String, TruncationInfo> truncateMap = new ConcurrentHashMap<>();

    private class TruncationInfo implements Runnable
    {
        String tablePattern;
        long rotationPeriod;
        int numRotations;
        long rescheduling;
        int maxAttempt;

        TruncationInfo(String t, long p, int n)
        {
            tablePattern = t;
            rotationPeriod = p;
            numRotations = n;
            rescheduling = rotationPeriod / 6;
            maxAttempt = 3;
        }

        public void run()
        {
            long cassandraTime = System.currentTimeMillis() + clockSkew;
            long runningPeriodIndex = cassandraTime / rotationPeriod;
            int nextTableIndex = (int) ((runningPeriodIndex + 1) % numRotations);
            String tableName = tablePattern.replace("$(TID)", String.valueOf(nextTableIndex));
            retry(tableName, 0);
        }

        public void retry(String tableName, int attempt)
        {
            if (attempt > maxAttempt) {
                logger.error("Failed multiple attempts in cassandra cluster '%s' TRUNCATE %s. Giving up after %d attempts.", name, tableName, maxAttempt);
                return;
            }
            logger.info("Cassandra cluster '%s' executing TRUNCATE TABLE %s, attempt %d.", name, tableName, attempt);
            try {
                RegularStatement truncateStatement = new SimpleStatement("TRUNCATE TABLE " + tableName);
                truncateStatement.setConsistencyLevel(ConsistencyLevel.ALL);
                truncateStatement.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
                truncateStatement.setReadTimeoutMillis(TRUNCATE_TIMEOUT);
                getSessionWithTimeout().execute(truncateStatement);
            }
            catch (Exception x) {
                logger.error(x, "Cassandra cluster '%s': failed TRUNCATE %s attempt %d. Rescheduling in %s", name, tableName, attempt, Duration.ofMillis(rescheduling).toString());
                executorService.schedule(() -> retry(tableName, attempt + 1), rescheduling, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void scheduleTruncation(String tableName, long rotationPeriod, int numRotations)
    {
        TruncationInfo ti = new TruncationInfo(tableName, rotationPeriod, numRotations);
        if (truncateMap.putIfAbsent(tableName, ti) == null) {
            long cassandraTime = System.currentTimeMillis() + clockSkew;
            long timeRemainingInCurrent = rotationPeriod - cassandraTime % rotationPeriod;
            long delay = timeRemainingInCurrent + rotationPeriod / 2;
            executorService.scheduleAtFixedRate(ti, delay, rotationPeriod, TimeUnit.MILLISECONDS);
            logger.info("Cassandra cluster '%s' scheduling truncations of %s. at %s (in %d ms) with period %s", name, tableName, new Date(System.currentTimeMillis() + delay), delay, Duration.ofMillis(rotationPeriod));
        }
    }

    interface SelectCassandraTimestamp extends SimpleCqlMapper<SelectCassandraTimestamp>
    {
        SimpleCqlFactory<SelectCassandraTimestamp> Factory = SimpleCqlFactory.factory(SelectCassandraTimestamp.class,
                "select now() as timeuuid FROM system.local")
                .setIdempotent(true)
                .setConsistencyLevel(ConsistencyLevel.ONE);

        TimeUUID timeuuid();
    }

    public CassandraClusterConnector(CassandraProperties config)
    {
        this.config = config;
        String name = getNameFromCallerAnnotation();
        this.name = (name == null) ? config.getName() : name;
    }

    public CassandraClusterConnector(String name, Session session)
    {
        this.session.complete(requireNonNull(session, "session is null"));
        this.name = name;
        this.config = new CassandraProperties();
    }

    public static String getNameFromCallerAnnotation()
    {
        StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
        for (int i = 1; i < stElements.length; i++) {
            StackTraceElement ste = stElements[i];
            try {
                for (Method m : Class.forName(ste.getClassName()).getDeclaredMethods()) {
                    if (CassandraClusterConnector.class.equals(m.getReturnType())) {
                        Named googleInjectNamed = m.getDeclaredAnnotation(Named.class);
                        if (googleInjectNamed != null) {
                            return googleInjectNamed.value();
                        }
                    }
                }
            }
            catch (ClassNotFoundException e) {
                continue;
            }
        }
        return null;
    }

    public void initialize()
    {
        if (session.isDone() || connect()) {
            logger.info("Cassandra cluster '%s' initialization complete.", name);
        }
        else if (config.getConnectRetryPeriod().toMillis() != 0) {
            logger.info("Cassandra cluster '%s' initialization rescheduled.", name);
            executorService.schedule(this::initialize, config.getConnectRetryPeriod().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    public boolean isLive()
    {
        if (!session.isDone()) {
            return false;
        }
        Session csession = session.getNow(null);

        if (csession != null && !csession.isClosed()) {
            return csession.getCluster().getMetrics().getConnectedToHosts().getValue() > 0;
        }
        return false;
    }


    @VisibleForTesting
    public void initializeForTesting(Cluster cluster, Session s)
    {
        for (Consumer<Cluster> configurer : clusterConfigurers) {
            configurer.accept(cluster);
        }
        initPermits(cluster);
        session.complete(s);
        preparers.forEach(dao -> dao.accept(this));
    }

    SSLContext getTestingSSLContext()
            throws NoSuchAlgorithmException, KeyManagementException
    {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509ExtendedTrustManager()
                {
                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                            throws CertificateException
                    {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                            throws CertificateException
                    {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers()
                    {
                        return null;
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket)
                            throws CertificateException
                    {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket)
                            throws CertificateException
                    {
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine)
                            throws CertificateException
                    {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine)
                            throws CertificateException
                    {
                    }
                }
        };

        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        return sslContext;
    }

    SSLContext getSSLContext()
            throws KeyManagementException, KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException
    {
        if (config.getTruststorePath() != null && config.getTruststoreKey() != null) {
            final KeyStore keyStore = KeyStore.getInstance("JKS");
            try (final InputStream is = new FileInputStream(requireNonNull(config.getTruststorePath(), "getTruststorePath"))) {
                keyStore.load(is, requireNonNull(config.getTruststoreKey(), "getTruststoreKey").toCharArray());
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);
            TrustManager[] tm = tmf.getTrustManagers();

            SSLContext sslcontext = SSLContext.getInstance("SSL");
            sslcontext.init(null, tm, new SecureRandom());
            return sslcontext;
        }
        else {
            return SSLContext.getDefault();
        }
    }

    public void addConnectListener(Consumer<CassandraClusterConnector> consumer)
    {
        preparers.add(consumer);
        if (session.isDone()) {
            consumer.accept(this);
        }
    }

    public void addBuildListener(Consumer<Cluster> consumer)
    {
        clusterConfigurers.add(consumer);
        if (session.isDone()) {
            consumer.accept(this.session.join().getCluster());
        }
    }

    private boolean connect()
    {
        Cluster cluster = null;
        logger.info("Initializing Cassandra cluster '%s'", name);
        try {
            Cluster.Builder clusterBuilder = Cluster.builder()
                    .addContactPoints(config.getHostsAsArray())
                    .withPort(config.getPort())
                    .withQueryOptions(new QueryOptions()
                            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                            .setSerialConsistencyLevel(ConsistencyLevel.SERIAL))
//                    .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.get()
//                            .withLocalDc(config.getLocalDatacenter())
//                            .withUsedHostsPerRemoteDc(0)
//                            .build())
                    .withPoolingOptions(new PoolingOptions()
                            .setMaxRequestsPerConnection(HostDistance.LOCAL, config.getMaxRequestsPerConnectionLocal())
                            .setMaxRequestsPerConnection(HostDistance.REMOTE, config.getMaxRequestsPerConnectionRemote())
                    )
                    .withSocketOptions(new SocketOptions()
                            .setReadTimeoutMillis((int) config.getReadTimeout().toMillis())
                            .setConnectTimeoutMillis((int) config.getConnectTimeout().toMillis()))
                    .withRetryPolicy(FallthroughRetryPolicy.INSTANCE);
            if (config.getUseCassandraSSL()) {
                // TODO: configurable choice of Jdk vs Netty
                clusterBuilder.withSSL(JdkSSLOptions.builder()
                        .withSSLContext(getSSLContext())
                        .withCipherSuites(new String[]{"TLS_RSA_WITH_AES_128_CBC_SHA"})
                        .build());
            }
            if (config.getClusterUsername() != null) {
                clusterBuilder.withCredentials(config.getClusterUsername(), config.getClusterPassword());
            }

            cluster = clusterBuilder.build();
            for (Consumer<Cluster> configurer : clusterConfigurers) {
                configurer.accept(cluster);
            }
            final Session s = cluster.connect();

            initPermits(cluster);

            session.complete(s);
            sessionMap.put(name, s);

            SelectCassandraTimestamp.Factory.prepare(this);
            preparers.forEach(dao -> dao.accept(this));
        }
        catch (NoHostAvailableException ne) {
            closeCluster(cluster);
            logger.error(ne, "while connecting");
        }
        catch (Exception e) {
            closeCluster(cluster);
            logger.error(e, "while connecting");
            throw new RuntimeException("Unrecoverable connect exception", e);
        }
        return session.isDone();
    }

    // need to provide backpressure to avoid BusyPoolException.
    // see also https://gist.github.com/burmanm/230c306f88c69c62dfe737994444fc01
    // see also https://stackoverflow.com/questions/47900973/com-datastax-driver-core-exceptions-busypoolexception
    void initPermits(Cluster cluster)
    {
        // TODO: assuming coordinator always goes to LOCAL hosts. By default, local hosts have max requests limit of 1024, and remote 256.
        int maxOutstandingRequests = cluster.getConfiguration().getPoolingOptions().getMaxRequestsPerConnection(HostDistance.LOCAL) + cluster.getConfiguration().getPoolingOptions().getMaxQueueSize()/3;
        logger.info("Cassandra max outstanding requests: %d", maxOutstandingRequests);

        permits = new Semaphore(maxOutstandingRequests);
    }

    public final void acquireRequestPermit()
    {
        permits.acquireUninterruptibly();
    }

    public final void releaseRequestPermit()
    {
        permits.release();
    }

    /**
     * fetches the clock skew between System.currentTimeMillis() and the cassandra server side.
     * the Cassandra timestamp then can be computed as System.currentTimeMillis()+getServerSideTimestampSkew()
     */
    public static long getServerSideTimestampSkew(Session s)
    {
        long minRoundtrip = Long.MAX_VALUE;
        long skew = 0;
        for (int i = 0; i < 5; i++) {
            long ts = System.currentTimeMillis();
            TimeUUID timeuuid = SelectCassandraTimestamp.Factory.get().executeAsyncAndMapOne().join().get().timeuuid();
            long now = System.currentTimeMillis();
            ts = now - ts;
            if (ts < minRoundtrip) {
                minRoundtrip = ts;
                skew = timeuuid.timestamp() - (now - minRoundtrip / 2);
            }
        }
        logger.debug("Fetched skew %d via round trip time %d", skew, minRoundtrip);
        return skew;
    }

    public long getServerSideTimestampSkew(boolean refresh)
    {
        if (clockSkew == 0 || refresh) {
            clockSkew = getServerSideTimestampSkew(getSessionWithTimeout());
        }
        return clockSkew;
    }

    private void closeCluster(Cluster cluster)
    {
        if (cluster != null) {
            try {
                cluster.closeAsync();
            }
            catch (RuntimeException re) {
                logger.error(re, "While closing cluster");
            }
        }
    }

    @Nonnull
    public Session getSession(long time, TimeUnit unit)
    {
        try {
            return session.get(time, unit);
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Exception while executing Cassandra connect", e.getCause());
        }
        catch (TimeoutException e) {
            throw new RuntimeException("Timeout while waiting for Cassandra connect", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupt while waiting for Cassandra connect", e);
        }
    }

    @Nonnull
    public Session getSessionWithTimeout()
    {
        return getSession(30, TimeUnit.SECONDS);
    }

    @Nonnull
    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    public Session getSession()
    {
        return requireNonNull(session.getNow(null), "Not connected: cluster " + name);
    }

    @Nonnull
    public static Session getSession(String clusterName)
    {
        if (!sessionMap.containsKey(clusterName)) {
            throw new RuntimeException(String.format("%s is not a valid cluster name", clusterName));
        }
        return sessionMap.get(clusterName);
    }

    @PreDestroy
    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    public synchronized void closeCluster()
    {
        if (executorService != null) {
            executorService.shutdownNow();
        }
        Session s = session.getNow(null);
        if (s != null) {
            s.close();
        }
    }

    public PreparedStatement prepareStatement(RegularStatement stmt)
            throws InvalidQueryException
    {
        Session session = getSession();
        return session.prepare(stmt);
    }
}

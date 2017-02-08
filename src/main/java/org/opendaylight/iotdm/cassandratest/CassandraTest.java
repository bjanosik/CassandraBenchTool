package org.opendaylight.iotdm.cassandratest;

import static java.lang.Thread.sleep;
import com.datastax.driver.core.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

public class CassandraTest
{
    private Cluster cluster;
    static Session s;
    private PreparedStatement createResource;
    private PreparedStatement getResourceById;
    private PreparedStatement updateJsonResourceContentString;
    private PreparedStatement createParentChildLink;
    private ExecutorService executor;
    private Integer nextQueueId = 0;
    private Integer nextQueueParentId = 0;
    private Integer numSuccessful = 0;
    private Integer numComplete = 0;
    public long createsPerSec;
    private ArrayList<ArrayList<Integer>> resourceIdQueues;
    private ArrayList<ArrayList<Integer>> resourceIdParentQueues;
    private boolean doReads = false;
    private int replicationFactor = 1;
    private static Scanner scanner;
    private static List<String> hostList = new ArrayList<>(Arrays.asList("127.0.0.1"));
    private static int numThreads = 128;
    private static int numResources = 5000;

    private synchronized Integer getNextQ() {
        return nextQueueId++;
    }
    private synchronized Integer getNextParentQ() {
        return nextQueueParentId++;
    }
    private synchronized void incNumSuccessful() { ++numSuccessful; }
    private synchronized void incNumComplete() { ++numComplete; }

    public static void main(String[] args) {
        scanner = new Scanner(System.in);
        CassandraTest ct = new CassandraTest();
        boolean runAgain = true;
        while (runAgain) {
            ct.promptForConfigChange();
            runAgain = false;
            ct.printArgValues();
            ct.runInitTests(hostList);
            ct.runTest1(numThreads, numResources);
            ct.runInitTests(hostList);
            ct.runTest2(numThreads, numResources);
            System.out.print("Run more tests? [y/n]: ");
            scanner.nextLine();
            String moreTests = scanner.nextLine();
            if (moreTests.equals("y") || moreTests.equals("Y")
                    || moreTests.equals("1") || moreTests.equals("true")) {
                runAgain = true;
                ct.reinitialize();
            }
            s.close();
            System.out.println("Done!");
        }
    }

    private void reinitialize() {
        nextQueueId = 0;
        nextQueueParentId = 0;
        numSuccessful = 0;
        numComplete = 0;
        resourceIdQueues.clear();
        resourceIdParentQueues.clear();
    }

    private void promptForConfigChange() {
        printArgValues();
        System.out.println("[0] FOR NO MODIFICATIONS");
        System.out.print("Value to change: ");
        int option = scanner.nextInt();

        while(option != 0) {
            modifyArgValue(option);
            System.out.print("Value to change: ");
            option = scanner.nextInt();
        }
    }

    private void printArgValues() {
        String hosts = "";
        System.out.println();
        System.out.println("Your test configuration: ");
        System.out.println("-----------------------------------");
        if(hostList.size() == 1) hosts += hostList.get(0);
        else {
            for(String hostIP : hostList) hosts += hostIP + "/";
        }
        System.out.println("[1] Host list: " + hosts);
        System.out.println("[2] Replication factor: " + replicationFactor);
        System.out.println("[3] Do reads: " + doReads);
        System.out.println("[4] Number of threads: " + numThreads);
        System.out.println("[5] Number of resources: " + numResources);
        System.out.println();
    }

    private void modifyArgValue(int argument) {
        scanner.nextLine();
        switch (argument) {

            case 1: System.out.print("[1] Host list: ");
                    hostList.clear();
                    String hostsArg = scanner.nextLine();
                    String[] IPs = hostsArg.split("/");
                    if(IPs.length > 1) {
                        for (String host : IPs) {
                            hostList.add(host);
                        }
                    }
                    else hostList.add(IPs[0]);
                    break;
            case 2: System.out.print("[2] Replication factor: ");
                    replicationFactor = scanner.nextInt();
                    alterReplicationFactor();
                    break;
            case 3: System.out.print("[3] Do reads: ");
                    doReads = scanner.nextBoolean();
                    break;
            case 4: System.out.print("[4] Number of threads: ");
                    numThreads = scanner.nextInt();
                    break;
            case 5: System.out.print("[5] Number of resources: ");
                    numResources = scanner.nextInt();
                    break;
            default: break;
        }
    }

    private void buildResourceIdQueues(int numQueues, int numResources) {
        resourceIdQueues = new ArrayList<ArrayList<Integer>>(numQueues);
        resourceIdParentQueues = new ArrayList<ArrayList<Integer>>(numQueues);
        for (int i = 0; i < numQueues; ++i) {
            ArrayList<Integer> resourceArray = new ArrayList<Integer>(numResources / numQueues + 1);
            resourceIdQueues.add(resourceArray);
            ArrayList<Integer> resourceParentArray = new ArrayList<Integer>(numResources / numQueues + 1);
            resourceIdParentQueues.add(resourceParentArray);
        }
        for (int i = 0; i < numResources; i++) {
            int q = i % numQueues;
            ArrayList<Integer> resourceArray = resourceIdQueues.get(q);
            resourceArray.add(i + 1);
            ArrayList<Integer> resourceParentArray = resourceIdParentQueues.get(q);
            resourceParentArray.add(i + 1);
        }
    }

    private void runInitTests(List<String> hosts) {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
                .setConnectionsPerHost(HostDistance.REMOTE, 2, 4)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
        Cluster.Builder clusterBuilder = Cluster.builder();
        for (String host : hosts) clusterBuilder = clusterBuilder.addContactPoint(host);
        clusterBuilder = clusterBuilder.withPoolingOptions(poolingOptions);
        cluster = clusterBuilder.build();

        initializeDatastore();
        try {
            if (s != null) {
                s.close();
            }
            s = cluster.connect("onem2m");
        } catch (Exception e) {
            System.out.println("Issues creating default db shards: " + e);
        }

        createResource = s.prepare(
                "INSERT INTO Resources " +
                "(resourceId,resourceType,resourceName,jsonContent,parentId,parentTargetUri) " +
                "values (?,?,?,?,?,?)");

        updateJsonResourceContentString = s.prepare(
                "UPDATE Resources SET jsonContent = ? " +
                "WHERE resourceId = ?");

        createParentChildLink = s.prepare(
                "INSERT INTO Children (parentResourceId, childName, childResourceId) " +
                "VALUES(?,?,?)");

        getResourceById = s.prepare("SELECT " +
                "resourceType,resourceName,jsonContent,parentId,parentTargetUri FROM Resources" +
                " WHERE resourceId = ?");
    }

    private void createParentsParallely(int numThreads, int numResources) {
        System.out.println("Creating parents in parallel");
        executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    writeParents(getNextParentQ());
                }
            });
        }

        while (numSuccessful != numResources) {
            try {
                sleep(1, 0);
            } catch (InterruptedException e) {
                System.out.println("sleep error: " +  e);
            }
        }
        executor.shutdown();
        System.out.println("Parents created " + numSuccessful + "/" + numResources);
        numSuccessful = 0;
        nextQueueParentId = 0;
    }

    private void writeParents(Integer q) {
        ArrayList<Integer> resourceIdArray = resourceIdParentQueues.get(q);
        for (Integer resourceId : resourceIdArray) {
            try {
                BoundStatement bound = createResource.bind(
                        "TEST_CONTAINER_" + resourceId.toString(),
                        "RT",
                        "TEST_CONTAINER_" + resourceId.toString(),
                        "012345678900123456789001234567890012345678900123456789001234567890012345678900123456789001234567890",
                        "TEST_CONTAINER_" + resourceId.toString(),
                        "/SYS_PERF_TEST_CSE/BASE_CONTAINER_1/");
                s.execute(bound);
                incNumSuccessful();
            } catch (Exception e) {
                System.out.println("createResource: Cassandra could not create resource " + e.getMessage());
            }
        }
    }

    private void runCreateTests1(Integer q) {
        ArrayList<Integer> resourceIdArray = resourceIdQueues.get(q);
        for (Integer resourceId : resourceIdArray) {
            try {
                BatchStatement bs = new BatchStatement(BatchStatement.Type.UNLOGGED);
                if(doReads) {
                    BoundStatement bound0 = getResourceById.bind("TEST_CONTAINER_" + resourceId.toString());
                    Iterator<Row> iterator = s.execute(bound0).iterator();
                    while(iterator.hasNext()){
                        Row row = iterator.next();
                        String resourceType = row.getString(0);
                        String name = row.getString(1);
                        String resourceContentJsonString = row.getString(2);
                        String parentId = row.getString(3);
                        String parentTargetURI = row.getString(4);
                        if(resourceType != null && name != null &&
                                resourceContentJsonString != null && parentId != null && parentTargetURI != null) {
                        }
                        else {
                            System.out.println("Reading failed!");
                        }
                    }
                }
                BoundStatement bound = createResource.bind(
                        "RN_" + resourceId.toString(),
                        "RT",
                        "RN_" + resourceId.toString(),
                        "012345678900123456789001234567890012345678900123456789001234" +
                                "567890012345678900123456789001234567890",
                        "PARENT_" + resourceId.toString(),
                        "/SYS_PERF_TEST_CSE/BASE_CONTAINER_1/TEST_CONTAINER_" + resourceId.toString());
                bs.add(bound);

                bound = updateJsonResourceContentString.bind("01234567890012345678900123456789001234567890012345" +
                                "6789001234567890012345678900123456789001234567890",
                        "TEST_CONTAINER_" + resourceId.toString());
                bs.add(bound);

                bound = createParentChildLink.bind("TEST_CONTAINER_" + resourceId.toString(),
                        "RN_" + resourceId.toString(), "RN_" + resourceId.toString());
                bs.add(bound);

                ResultSetFuture future = s.executeAsync(bs);
                Futures.addCallback(future,
                    new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet rows) {
                            incNumComplete();
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            System.out.println("Write failed" + throwable.getMessage());
                        }
                    },
                    MoreExecutors.sameThreadExecutor());
            } catch (Exception e) {
                System.out.println("createResource: Cassandra could not create resource " + e.getMessage());
            }
        }
    }

    private void runCreateTests2(Integer q) {
        ArrayList<Integer> resourceIdArray = resourceIdQueues.get(q);
        for (Integer resourceId : resourceIdArray) {
            try {
                if(doReads) {
                    BoundStatement bound0 = getResourceById.bind("TEST_CONTAINER_" + resourceId.toString());
                    Iterator<Row> iterator = s.execute(bound0).iterator();
                    while(iterator.hasNext()){
                        Row row = iterator.next();
                        String resourceType = row.getString(0);
                        String name = row.getString(1);
                        String resourceContentJsonString = row.getString(2);
                        String parentId = row.getString(3);
                        String parentTargetURI = row.getString(4);
                        if(resourceType != null && name != null &&
                                resourceContentJsonString != null && parentId != null && parentTargetURI != null)
                            incNumSuccessful();
                        else System.out.println("Reading failed!");
                    }
                }
                BoundStatement bound = createResource.bind(
                        "RN_" + resourceId.toString(),
                        "RT",
                        "RN_" + resourceId.toString(),
                        "012345678900123456789001234567890012345678900123456789001234567890012345678900123456789001234567890",
                        "PARENT_" + resourceId.toString(),
                        "/SYS_PERF_TEST_CSE/BASE_CONTAINER_1/TEST_CONTAINER_" + resourceId.toString());
                ResultSetFuture future = s.executeAsync(bound);
                Futures.addCallback(future,
                    new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet rows) {
                            incNumSuccessful();
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            System.out.println("Write failed" + throwable.getMessage());
                        }

                    },
                    MoreExecutors.sameThreadExecutor());


                bound = updateJsonResourceContentString.bind("012345678900123456789001234567890012345678900" +
                                "123456789001234567890012345678900123456789001234567890",
                        "TEST_CONTAINER_" + resourceId.toString());
                ResultSetFuture future1 = s.executeAsync(bound);
                Futures.addCallback(future1,
                    new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet rows) {
                            incNumSuccessful();
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            System.out.println("Write failed" + throwable.getMessage());
                        }
                    },
                    MoreExecutors.sameThreadExecutor());

                bound = createParentChildLink.bind("TEST_CONTAINER_" + resourceId.toString(),
                        "RN_" + resourceId.toString(), "RN_" + resourceId.toString());
                ResultSetFuture future3 = s.executeAsync(bound);
                Futures.addCallback(future3,
                    new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet rows) {
                            incNumSuccessful();
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            System.out.println("Write failed" + throwable.getMessage());
                        }
                    },
                    MoreExecutors.sameThreadExecutor());
            } catch (Exception e) {
                System.out.println("createResource: Cassandra could not create resource " + e.getMessage());
            }
        }
    }

    private void runTest1(int numThreads, int numResources) {

        long startTime, endTime, delta;
        nextQueueId = 0;
        numComplete = 0;
        numSuccessful = 0;

        buildResourceIdQueues(numThreads, numResources);
        createParentsParallely(numThreads, numResources);
        executor = Executors.newFixedThreadPool(numThreads);
        startTime = System.nanoTime();

        for (int i = 0; i < numThreads; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    runCreateTests1(getNextQ());
                }
            });
        }

        while (numComplete != numResources) {
            try {
                sleep(1, 0);
            } catch (InterruptedException e) {
                System.out.println("sleep error: " + e);
            }
        }
        endTime = System.nanoTime();
        delta = (endTime - startTime);
        createsPerSec = nPerSecond(numResources, delta);
        System.out.println();
        System.out.println("Batch Statement test");
        System.out.println("-----------------------------------");
        System.out.println("Time to create ... numComplete: " + numComplete);
        System.out.println("Time to create ... numResources: " + numResources);
        System.out.println("Time to create ... delta: " + delta);
        System.out.println("Time to create ... createsPerSec: " + createsPerSec);
        System.out.println("-----------------------------------");
        System.out.println();

        executor.shutdown(); // kill the threads
    }

    private void runTest2(int numThreads, int numResources) {

        long startTime, endTime, delta;
        nextQueueId = 0;
        numComplete = 0;
        numSuccessful = 0;

        buildResourceIdQueues(numThreads, numResources);
        createParentsParallely(numThreads, numResources);
        executor = Executors.newFixedThreadPool(numThreads);
        startTime = System.nanoTime();

        for (int i = 0; i < numThreads; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    runCreateTests2(getNextQ());
                }
            });
        }

        int mod;
        if (doReads) mod = 4;
        else mod = 3;

        while (numSuccessful / mod != numResources) {
            try {
                sleep(1, 0);
            } catch (InterruptedException e) {
                System.out.println("sleep error: " + e);
            }
        }

        endTime = System.nanoTime();
        delta = (endTime - startTime);
        createsPerSec = nPerSecond(numResources, delta);
        System.out.println();
        System.out.println("Test without the use of Batch Statement");
        System.out.println("-----------------------------------");
        System.out.println("Time to create ... numComplete: " + numSuccessful / mod);
        System.out.println("Time to create ... numResources: " + numResources);
        System.out.println("Time to create ... delta: " + delta);
        System.out.println("Time to create ... createsPerSec: " + createsPerSec);
        System.out.println("-----------------------------------");
        System.out.println();

        executor.shutdown(); // kill the threads

    }

    private void initializeDatastore() {
        dropEverything();
        createIfNotExistsKeyspaceAndTables();
    }

    private void dropEverything() {
        try (Session s = cluster.connect()) {
            String com = "DROP KEYSPACE if exists onem2m";
            s.execute(com);
            s.close();
        } catch (Exception e) {
            System.out.println("Failed to clear keyspace: " + e);
        }
    }

    private void createIfNotExistsKeyspaceAndTables() {
        try (Session s = cluster.connect()) {
            String com;

            com = "CREATE KEYSPACE  IF NOT EXISTS  onem2m " +
                    "WITH replication = " +
                    "{ 'class' : 'SimpleStrategy', 'replication_factor' : " + replicationFactor + "}";
            s.execute(com);

            com = "USE onem2m";
            s.execute(com);

            com = "CREATE TABLE IF NOT EXISTS Resources (" +
                    "resourceId TEXT PRIMARY KEY," +
                    "resourceType TEXT," +
                    "parentTargetUri TEXT," +
                    "resourceName TEXT," +
                    "jsonContent TEXT," +
                    "parentId TEXT" +
                    ")";
            s.execute(com);

            com = "CREATE TABLE IF NOT EXISTS Children (" +
                    "parentResourceId TEXT," +
                    "childName TEXT," +
                    "childResourceId TEXT," +
                    "PRIMARY KEY ((parentResourceId), childName)" +
                    ")";
            s.execute(com);
            s.close();

        } catch (Exception e) {
            System.out.println("createIfNotExistsKeyspacePlusNonShardedTables; Failed ...: " + e);
        }
    }

    private void alterReplicationFactor() {
            String com = "ALTER KEYSPACE onem2m " +
                    "WITH replication = " +
                    "{ 'class' : 'SimpleStrategy', 'replication_factor' : " + replicationFactor + "}";
            s.execute(com);
    }

    private long nPerSecond(int num, long delta) {

        double secondsTotal = (double) delta / (double) 1000000000;
        return (long) (((double) num / secondsTotal));
    }
}
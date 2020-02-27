/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.clustering;

import static io.zeebe.broker.Broker.LOG;
import static io.zeebe.broker.test.EmbeddedBrokerConfigurator.DEBUG_EXPORTER;
import static io.zeebe.broker.test.EmbeddedBrokerConfigurator.DISABLE_EMBEDDED_GATEWAY;
import static io.zeebe.broker.test.EmbeddedBrokerConfigurator.TEST_RECORDER;
import static io.zeebe.broker.test.EmbeddedBrokerConfigurator.setCluster;
import static io.zeebe.broker.test.EmbeddedBrokerConfigurator.setInitialContactPoints;
import static io.zeebe.broker.test.EmbeddedBrokerRule.assignSocketAddresses;
import static io.zeebe.protocol.Protocol.START_PARTITION_ID;
import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.test.util.TestUtil.waitUntil;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.protocol.SwimMembershipProtocol;
import io.atomix.core.Atomix;
import io.atomix.utils.net.Address;
import io.zeebe.broker.Broker;
import io.zeebe.broker.PartitionListener;
import io.zeebe.broker.clustering.atomix.AtomixFactory;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.ZeebeClientBuilder;
import io.zeebe.client.api.response.BrokerInfo;
import io.zeebe.client.api.response.PartitionInfo;
import io.zeebe.client.api.response.Topology;
import io.zeebe.gateway.Gateway;
import io.zeebe.gateway.impl.broker.request.BrokerCreateWorkflowInstanceRequest;
import io.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.zeebe.gateway.impl.configuration.ClusterCfg;
import io.zeebe.gateway.impl.configuration.GatewayCfg;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceCreationRecord;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import io.zeebe.test.util.socket.SocketUtil;
import io.zeebe.transport.impl.SocketAddress;
import io.zeebe.util.exception.UncheckedExecutionException;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.clock.ControlledActorClock;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class ClusteringRule extends ExternalResource {

  private static final int TOPOLOGY_RETRIES = 250;
  private static final AtomicLong CLUSTER_COUNT = new AtomicLong(0);
  private static final boolean ENABLE_DEBUG_EXPORTER = false;
  private static final String RAFT_PARTITION_PATH = AtomixFactory.GROUP_NAME + "/partitions/1";

  private final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();
  private final AutoCloseableRule closeables = new AutoCloseableRule();
  private final TemporaryFolder temporaryFolder = new TemporaryFolder();

  // configuration
  private final int partitionCount;
  private final int replicationFactor;
  private final int clusterSize;
  private final Consumer<BrokerCfg> brokerConfigurator;
  private final Consumer<GatewayCfg> gatewayConfigurator;
  private final Consumer<ZeebeClientBuilder> clientConfigurator;
  private final Map<Integer, Broker> brokers;
  private final Map<Integer, BrokerCfg> brokerCfgs;
  private final List<Integer> partitionIds;
  private final String clusterName;
  private final ControlledActorClock controlledClock = new ControlledActorClock();
  private final Map<Integer, LogStream> logstreams = new ConcurrentHashMap<>();

  // cluster
  private ZeebeClient client;
  private Gateway gateway;
  private LeaderListener leaderListener;

  public ClusteringRule() {
    this(3);
  }

  public ClusteringRule(final int clusterSize) {
    this(clusterSize, clusterSize, clusterSize);
  }

  public ClusteringRule(
      final int partitionCount, final int replicationFactor, final int clusterSize) {
    this(partitionCount, replicationFactor, clusterSize, cfg -> {});
  }

  public ClusteringRule(
      final int partitionCount,
      final int replicationFactor,
      final int clusterSize,
      final Consumer<BrokerCfg> configurator) {
    this(
        partitionCount,
        replicationFactor,
        clusterSize,
        configurator,
        gatewayCfg -> {},
        ZeebeClientBuilder::usePlaintext);
  }

  public ClusteringRule(
      final int partitionCount,
      final int replicationFactor,
      final int clusterSize,
      final Consumer<BrokerCfg> brokerConfigurator,
      final Consumer<GatewayCfg> gatewayConfigurator,
      final Consumer<ZeebeClientBuilder> clientConfigurator) {
    this.partitionCount = partitionCount;
    this.replicationFactor = replicationFactor;
    this.clusterSize = clusterSize;
    this.brokerConfigurator = brokerConfigurator;
    this.gatewayConfigurator = gatewayConfigurator;
    this.clientConfigurator = clientConfigurator;

    brokers = new HashMap<>();
    brokerCfgs = new HashMap<>();
    this.partitionIds =
        IntStream.range(START_PARTITION_ID, START_PARTITION_ID + partitionCount)
            .boxed()
            .collect(Collectors.toList());

    clusterName = "zeebe-cluster-" + CLUSTER_COUNT.getAndIncrement();
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public int getClusterSize() {
    return clusterSize;
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    Statement statement = recordingExporterTestWatcher.apply(base, description);
    statement = closeables.apply(statement, description);
    return temporaryFolder.apply(super.apply(statement, description), description);
  }

  @Override
  protected void before() throws IOException {
    leaderListener = new LeaderListener(partitionCount);
    // create brokers
    for (int nodeId = 0; nodeId < clusterSize; nodeId++) {
      getBroker(nodeId);
    }

    // create gateway
    gateway = createGateway();
    gateway.start();

    // create client
    client = createClient();

    try {
      waitUntilBrokersStarted();
      waitForPartitionReplicationFactor();
      LOG.info("Full replication factor");
      waitUntilBrokersInTopology();
      LOG.info("All brokers in topology {}", getTopologyFromClient());

    } catch (final Exception e) {
      // If the previous waits timeouts, the brokers are not closed automatically.
      after();
      throw new UncheckedExecutionException("Cluster start failed", e);
    }
  }

  @Override
  protected void after() {
    LOG.debug("Closing ClusteringRule...");
    brokers.values().parallelStream().forEach(Broker::close);
    brokers.clear();
    brokerCfgs.clear();
  }

  public Broker getBroker(final int nodeId) {
    return brokers.computeIfAbsent(nodeId, this::createBroker);
  }

  private void waitUntilBrokersStarted()
      throws InterruptedException, TimeoutException, ExecutionException {
    final var brokerStartFutures =
        brokers.values().parallelStream().map(Broker::start).toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(brokerStartFutures).get(120, TimeUnit.SECONDS);

    leaderListener.awaitLeaders();
  }

  private Broker createBroker(final int nodeId) {
    final File brokerBase = getBrokerBase(nodeId);
    final BrokerCfg brokerCfg = getBrokerCfg(nodeId);
    final Broker broker = new Broker(brokerCfg, brokerBase.getAbsolutePath(), controlledClock);
    broker.addPartitionListener(leaderListener);
    new Thread(broker::start).start();
    return broker;
  }

  private BrokerCfg getBrokerCfg(final int nodeId) {
    return brokerCfgs.computeIfAbsent(nodeId, this::createBrokerCfg);
  }

  private BrokerCfg createBrokerCfg(final int nodeId) {
    final BrokerCfg brokerCfg = new BrokerCfg();

    // build-in exporters
    if (ENABLE_DEBUG_EXPORTER) {
      DEBUG_EXPORTER.accept(brokerCfg);
    }
    TEST_RECORDER.accept(brokerCfg);

    // disable embedded gateway
    DISABLE_EMBEDDED_GATEWAY.accept(brokerCfg);

    // configure cluster
    setCluster(nodeId, partitionCount, replicationFactor, clusterSize, clusterName)
        .accept(brokerCfg);
    if (nodeId > 0) {
      // all nodes have to join the same broker
      // https://github.com/zeebe-io/zeebe/issues/2012

      setInitialContactPoints(getBrokerCfg(0).getNetwork().getInternalApi().getAddress().toString())
          .accept(brokerCfg);
    }

    // custom configurators
    brokerConfigurator.accept(brokerCfg);

    // set random port numbers
    assignSocketAddresses(brokerCfg);

    return brokerCfg;
  }

  private File getBrokerBase(final int nodeId) {
    final var base = new File(temporaryFolder.getRoot(), String.valueOf(nodeId));
    if (!base.exists()) {
      base.mkdir();
    }

    return base;
  }

  private Gateway createGateway() {
    final String contactPoint =
        getBrokerCfg(0).getNetwork().getInternalApi().getAddress().toString();

    final GatewayCfg gatewayCfg = new GatewayCfg();
    gatewayCfg.getCluster().setContactPoint(contactPoint).setClusterName(clusterName);
    gatewayCfg.getNetwork().setPort(SocketUtil.getNextAddress().getPort());
    gatewayCfg.getCluster().setPort(SocketUtil.getNextAddress().getPort());
    // temporarily increase request time out, but we should make this configurable per test
    gatewayCfg.getCluster().setRequestTimeout(Duration.ofSeconds(45));
    gatewayCfg.init();

    gatewayConfigurator.accept(gatewayCfg);

    final ClusterCfg clusterCfg = gatewayCfg.getCluster();

    // copied from StandaloneGateway
    final AtomixCluster atomixCluster =
        Atomix.builder()
            .withMemberId(clusterCfg.getMemberId())
            .withAddress(Address.from(clusterCfg.getHost(), clusterCfg.getPort()))
            .withClusterId(clusterCfg.getClusterName())
            .withMembershipProvider(
                BootstrapDiscoveryProvider.builder()
                    .withNodes(Address.from(clusterCfg.getContactPoint()))
                    .build())
            .withMembershipProtocol(
                SwimMembershipProtocol.builder().withSyncInterval(Duration.ofSeconds(1)).build())
            .build();

    atomixCluster.start().join();

    final ActorScheduler actorScheduler =
        ActorScheduler.newActorScheduler().setCpuBoundActorThreadCount(1).build();

    actorScheduler.start();

    final Gateway gateway = new Gateway(gatewayCfg, atomixCluster, actorScheduler);
    closeables.manage(gateway::stop);
    closeables.manage(atomixCluster::stop);
    closeables.manage(actorScheduler::stop);
    return gateway;
  }

  private ZeebeClient createClient() {
    final String contactPoint = gateway.getGatewayCfg().getNetwork().toSocketAddress().toString();
    final ZeebeClientBuilder zeebeClientBuilder =
        ZeebeClient.newClientBuilder().brokerContactPoint(contactPoint);

    clientConfigurator.accept(zeebeClientBuilder);

    final ZeebeClient client = zeebeClientBuilder.build();
    closeables.manage(client);
    return client;
  }

  private void waitUntilBrokersInTopology() {

    final Set<SocketAddress> addresses =
        brokers.values().stream()
            .map(Broker::getConfig)
            .map(b -> b.getNetwork().getCommandApi().getAddress())
            .collect(Collectors.toSet());

    waitForTopology(
        topology ->
            topology.stream()
                .map(b -> new SocketAddress(b.getHost(), b.getPort()))
                .collect(Collectors.toSet())
                .containsAll(addresses));
  }

  public Topology getTopologyFromClient() {
    return doRepeatedly(
            () -> {
              try {
                return client.newTopologyRequest().send().join();
              } catch (Exception e) {
                LOG.trace("Topology request failed: ", e);
                return null;
              }
            })
        .until(Objects::nonNull);
  }

  /**
   * Returns the current leader for the given partition.
   *
   * @param partition
   * @return
   */
  public BrokerInfo getLeaderForPartition(final int partition) {
    return doRepeatedly(
            () -> {
              final List<BrokerInfo> brokers =
                  client.newTopologyRequest().send().join().getBrokers();
              return extractPartitionLeader(brokers, partition);
            })
        .until(Optional::isPresent)
        .get();
  }

  private Optional<BrokerInfo> extractPartitionLeader(
      final List<BrokerInfo> brokers, final int partition) {
    return brokers.stream()
        .filter(
            b ->
                b.getPartitions().stream()
                    .anyMatch(p -> p.getPartitionId() == partition && p.isLeader()))
        .findFirst();
  }

  /** Wait for a partition bootstrap in the cluster. */
  public void waitForPartitionReplicationFactor() {
    waitForTopology(
        topology ->
            hasPartitionsWithReplicationFactor(topology, partitionCount, replicationFactor));
  }

  private boolean hasPartitionsWithReplicationFactor(
      final List<BrokerInfo> brokers, final int partitionCount, final int replicationFactor) {
    final AtomicLong leaders = new AtomicLong();
    final AtomicLong followers = new AtomicLong();

    brokers.stream()
        .flatMap(b -> b.getPartitions().stream())
        .forEach(
            p -> {
              if (p.isLeader()) {
                leaders.getAndIncrement();
              } else {
                followers.getAndIncrement();
              }
            });

    return leaders.get() >= partitionCount
        && followers.get() >= partitionCount * (replicationFactor - 1);
  }

  /**
   * Restarts broker, if the broker is still running it will be closed before.
   *
   * <p>Returns to the user if the broker is back in the cluster.
   */
  public void restartBroker(final int nodeId) {
    stopBroker(nodeId);
    final Broker broker = getBroker(nodeId).start().join();
    final SocketAddress commandApi = broker.getConfig().getNetwork().getCommandApi().getAddress();
    waitUntilBrokerIsAddedToTopology(commandApi);
    waitForPartitionReplicationFactor();
  }

  private void waitUntilBrokerIsAddedToTopology(final SocketAddress socketAddress) {
    waitForTopology(
        topology ->
            topology.stream()
                .anyMatch(
                    b ->
                        b.getHost().equals(socketAddress.host())
                            && b.getPort() == socketAddress.port()));
  }

  /**
   * Returns for a given broker the leading partition id's.
   *
   * @param socketAddress
   * @return
   */
  public List<Integer> getBrokersLeadingPartitions(final SocketAddress socketAddress) {
    return client.newTopologyRequest().send().join().getBrokers().stream()
        .filter(
            b -> b.getHost().equals(socketAddress.host()) && b.getPort() == socketAddress.port())
        .flatMap(broker -> broker.getPartitions().stream())
        .filter(PartitionInfo::isLeader)
        .map(PartitionInfo::getPartitionId)
        .collect(Collectors.toList());
  }

  /**
   * Returns the list of available brokers in a cluster.
   *
   * @return
   */
  public List<SocketAddress> getBrokersInCluster() {
    return client.newTopologyRequest().send().join().getBrokers().stream()
        .map(b -> new SocketAddress(b.getHost(), b.getPort()))
        .collect(Collectors.toList());
  }

  public Collection<Broker> getBrokers() {
    return brokers.values();
  }

  public SocketAddress[] getOtherBrokers(final String address) {
    return getOtherBrokers(SocketAddress.from(address));
  }

  public SocketAddress[] getOtherBrokers(final SocketAddress address) {
    return getBrokers().stream()
        .map(b -> b.getConfig().getNetwork().getCommandApi().getAddress())
        .filter(a -> !address.equals(a))
        .toArray(SocketAddress[]::new);
  }

  public SocketAddress[] getOtherBrokers(final int nodeId) {
    final SocketAddress filter = getBrokerCfg(nodeId).getNetwork().getCommandApi().getAddress();
    return getOtherBrokers(filter);
  }

  /**
   * Returns the count of partition leaders
   *
   * @return
   */
  public long getPartitionLeaderCount() {
    return client.newTopologyRequest().send().join().getBrokers().stream()
        .flatMap(broker -> broker.getPartitions().stream())
        .filter(PartitionInfo::isLeader)
        .count();
  }

  public void stopBroker(final int nodeId) {
    final Broker broker = brokers.remove(nodeId);
    if (broker != null) {
      final SocketAddress socketAddress =
          broker.getConfig().getNetwork().getCommandApi().getAddress();
      final List<Integer> brokersLeadingPartitions = getBrokersLeadingPartitions(socketAddress);
      broker.close();

      waitUntilBrokerIsRemovedFromTopology(socketAddress);
      waitForNewLeaderOfPartitions(brokersLeadingPartitions, socketAddress);
    }
  }

  private void waitUntilBrokerIsRemovedFromTopology(final SocketAddress socketAddress) {
    waitForTopology(
        topology ->
            topology.stream()
                .noneMatch(
                    b ->
                        b.getHost().equals(socketAddress.host())
                            && b.getPort() == socketAddress.port()));
  }

  private void waitForNewLeaderOfPartitions(
      final List<Integer> partitions, final SocketAddress oldLeader) {
    waitForTopology(
        topology ->
            topology.stream()
                .filter(
                    b -> !(b.getHost().equals(oldLeader.host()) && b.getPort() == oldLeader.port()))
                .flatMap(broker -> broker.getPartitions().stream())
                .filter(PartitionInfo::isLeader)
                .map(PartitionInfo::getPartitionId)
                .collect(Collectors.toSet())
                .containsAll(partitions));
  }

  public void waitForTopology(final Predicate<List<BrokerInfo>> topologyPredicate) {
    waitUntil(
        () -> topologyPredicate.test(getTopologyFromClient().getBrokers()),
        TOPOLOGY_RETRIES,
        "Failed to wait for topology %s",
        getTopologyFromClient());
  }

  public long createWorkflowInstanceOnPartition(final int partitionId, final String bpmnProcessId) {
    final BrokerCreateWorkflowInstanceRequest request =
        new BrokerCreateWorkflowInstanceRequest().setBpmnProcessId(bpmnProcessId);

    request.setPartitionId(partitionId);

    final BrokerResponse<WorkflowInstanceCreationRecord> response =
        gateway.getBrokerClient().sendRequest(request).join();

    if (response.isResponse()) {
      return response.getResponse().getWorkflowInstanceKey();
    } else {
      throw new RuntimeException(
          "Failed to create workflow instance for bpmn process id "
              + bpmnProcessId
              + " on partition with id "
              + partitionId
              + ": "
              + response);
    }
  }

  public SocketAddress getGatewayAddress() {
    return gateway.getGatewayCfg().getNetwork().toSocketAddress();
  }

  public ZeebeClient getClient() {
    return client;
  }

  public ControlledActorClock getClock() {
    return controlledClock;
  }

  public List<Integer> getPartitionIds() {
    return partitionIds;
  }

  public List<Broker> getOtherBrokerObjects(final int leaderNodeId) {
    return brokers.keySet().stream()
        .filter(id -> id != leaderNodeId)
        .map(brokers::get)
        .collect(Collectors.toList());
  }

  public Path getSegmentsDirectory(final Broker broker) {
    final String dataDir = broker.getConfig().getData().getDirectories().get(0);
    return Paths.get(dataDir).resolve(RAFT_PARTITION_PATH);
  }

  public File getSnapshotsDirectory(final Broker broker) {
    final String dataDir = broker.getConfig().getData().getDirectories().get(0);
    return new File(dataDir, RAFT_PARTITION_PATH + "/snapshots");
  }

  public void waitForValidSnapshotAtBroker(final Broker broker) {
    final File snapshotsDir = getSnapshotsDirectory(broker);
    waitUntil(() -> Optional.ofNullable(snapshotsDir.listFiles()).map(f -> f.length).orElse(0) > 0);
  }

  public LogStream getLogStream(final int partitionId) {
    return logstreams.get(partitionId);
  }

  private class LeaderListener implements PartitionListener {

    final CountDownLatch latch;

    LeaderListener(final int partitionCount) {
      this.latch = new CountDownLatch(partitionCount);
    }

    @Override
    public void onBecomingFollower(
        final int partitionId, final long term, final LogStream logStream) {}

    @Override
    public void onBecomingLeader(
        final int partitionId, final long term, final LogStream logStream) {
      logstreams.put(partitionId, logStream);
      latch.countDown();
    }

    public void awaitLeaders() throws InterruptedException {
      latch.await(15, TimeUnit.SECONDS);
    }
  }
}

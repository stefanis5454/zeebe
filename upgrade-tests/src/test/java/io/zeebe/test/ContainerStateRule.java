/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test;

import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebePort;
import io.zeebe.containers.ZeebeStandaloneGatewayContainer;
import java.util.Arrays;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.testcontainers.containers.Network;

class ContainerStateRule implements TestRule {

  private static final int CLOSE_TIMEOUT = 15;
  private static final Logger LOG = LoggerFactory.getLogger(ContainerStateRule.class);
  private ZeebeBrokerContainer broker;
  private ZeebeStandaloneGatewayContainer gateway;
  private ZeebeClient client;
  private Network network;

  public ZeebeClient client() {
    return client;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          base.evaluate();
        } catch (Throwable t) {
          if (broker != null && LOG.isErrorEnabled()) {
            LOG.error(
                String.format(
                    "%n===============================================%nBroker logs%n===============================================%n%s",
                    broker.getLogs().replaceAll("\n\n", "\n")));
          }

          if (gateway != null && LOG.isErrorEnabled()) {
            LOG.error(
                String.format(
                    "%n===============================================%nGateway logs%n===============================================%n%s",
                    gateway.getLogs().replaceAll("\n\n", "\n")));
          }

          throw t;
        } finally {
          close();
        }
      }
    };
  }

  /**
   * Start a broker with an embedded gateway and create a client with the broker's contact point.
   */
  void startBrokerEmbeddedGateway(final String brokerVersion, final String volumePath) {
    network = Network.newNetwork();

    broker =
        new ZeebeBrokerContainer(brokerVersion)
            .withFileSystemBind(volumePath, "/usr/local/zeebe/data")
            .withNetwork(network)
            .withEmbeddedGateway(true)
            .withDebug(true)
            .withLogLevel(Level.DEBUG);
    broker.start();

    final String contactPoint = broker.getExternalAddress(ZeebePort.GATEWAY);
    client = ZeebeClient.newClientBuilder().brokerContactPoint(contactPoint).usePlaintext().build();
  }

  /** Start a broker, a standalone gateway and create a client with the gateway's contact point. */
  void startBrokerStandaloneGateway(
      final String brokerVersion, final String volumePath, final String gatewayVersion) {
    network = Network.newNetwork();

    broker =
        new ZeebeBrokerContainer(brokerVersion)
            .withFileSystemBind(volumePath, "/usr/local/zeebe/data")
            .withNetwork(network)
            .withEmbeddedGateway(true)
            .withDebug(true)
            .withLogLevel(Level.DEBUG);
    broker.start();

    gateway =
        new ZeebeStandaloneGatewayContainer(gatewayVersion)
            .withContactPoint(broker.getContactPoint())
            .withNetwork(network)
            .withLogLevel(Level.DEBUG);
    gateway.start();

    final String contactPoint = gateway.getExternalAddress(ZeebePort.GATEWAY);
    client = ZeebeClient.newClientBuilder().brokerContactPoint(contactPoint).usePlaintext().build();
  }

  /** @return true if the element was found in the specified intent. Otherwise, returns false */
  boolean findElementInState(final String elementId, final String intent) {
    return findLogContaining(
        String.format("\"elementId\":\"%s\"", elementId),
        String.format("\"intent\":\"%s\"", intent));
  }

  /** @return true if the message was found in the specified intent. Otherwise, returns false */
  boolean findMessageInState(final String name, final String intent) {
    return findLogContaining(
        String.format("\"name\":\"%s\"", name), String.format("\"intent\":\"%s\"", intent));
  }

  boolean findLogContaining(final String... piece) {
    final String[] lines = broker.getLogs().split("\n");

    for (int i = lines.length - 1; i >= 0; --i) {
      final int finalI = i;
      if (Arrays.stream(piece).allMatch(p -> lines[finalI].contains(p))) {
        return true;
      }
    }

    return false;
  }

  /** Close all opened resources. */
  void close() {
    if (client != null) {
      client.close();
      client = null;
    }

    if (gateway != null) {
      gateway.close();
      gateway = null;
    }

    if (broker != null) {
      broker
          .getDockerClient()
          .stopContainerCmd(broker.getContainerId())
          .withTimeout(CLOSE_TIMEOUT)
          .exec();
      broker.close();
      broker = null;
    }

    if (network != null) {
      network.close();
      network = null;
    }
  }
}

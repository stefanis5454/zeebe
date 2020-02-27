/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.impl.configuration;

import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_CERTIFICATE_PATH;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_CLUSTER_HOST;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_CLUSTER_MEMBER_ID;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_CLUSTER_NAME;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_CLUSTER_PORT;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_CONTACT_POINT;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_HOST;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_KEEP_ALIVE_INTERVAL;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_MANAGEMENT_THREADS;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_MAX_MESSAGE_SIZE;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_MONITORING_ENABLED;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_MONITORING_HOST;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_MONITORING_PORT;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_PORT;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_PRIVATE_KEY_PATH;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_REQUEST_TIMEOUT;
import static io.zeebe.gateway.impl.configuration.EnvironmentConstants.ENV_GATEWAY_SECURITY_ENABLED;

import io.zeebe.test.util.TestConfigurationFactory;
import io.zeebe.util.Environment;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public final class GatewayCfgTest {

  private static final String DEFAULT_CFG_FILENAME = "/configuration/gateway.default.yaml";
  private static final GatewayCfg DEFAULT_CFG = new GatewayCfg();
  private static final String EMPTY_CFG_FILENAME = "/configuration/gateway.empty.yaml";
  private static final String CUSTOM_CFG_FILENAME = "/configuration/gateway.custom.yaml";
  private static final GatewayCfg CUSTOM_CFG = new GatewayCfg();

  static {
    DEFAULT_CFG.init();
    CUSTOM_CFG.init();
    CUSTOM_CFG.getNetwork().setHost("192.168.0.1").setPort(123);
    CUSTOM_CFG
        .getCluster()
        .setContactPoint("foobar:1234")
        .setRequestTimeout("123h")
        .setClusterName("testCluster")
        .setMemberId("testMember")
        .setHost("1.2.3.4")
        .setPort(12321);
    CUSTOM_CFG.getThreads().setManagementThreads(100);
  }

  private final Map<String, String> environment = new HashMap<>();

  @Test
  public void shouldHaveDefaultValues() {
    // when
    final GatewayCfg gatewayCfg = readDefaultConfig();

    // then
    Assertions.assertThat(gatewayCfg).isEqualTo(DEFAULT_CFG);
  }

  @Test
  public void shouldLoadEmptyConfig() {
    // when
    final GatewayCfg gatewayCfg = readEmptyConfig();

    // then
    Assertions.assertThat(gatewayCfg).isEqualTo(DEFAULT_CFG);
  }

  @Test
  public void shouldLoadCustomConfig() {
    // when
    final GatewayCfg gatewayCfg = readCustomConfig();

    // then
    Assertions.assertThat(gatewayCfg).isEqualTo(CUSTOM_CFG);
  }

  @Test
  public void shouldUseEnvironmentVariables() {
    // given
    setEnv(ENV_GATEWAY_HOST, "zeebe");
    setEnv(ENV_GATEWAY_PORT, "5432");
    setEnv(ENV_GATEWAY_CONTACT_POINT, "broker:432");
    setEnv(ENV_GATEWAY_MAX_MESSAGE_SIZE, "1G");
    setEnv(ENV_GATEWAY_MANAGEMENT_THREADS, "32");
    setEnv(ENV_GATEWAY_REQUEST_TIMEOUT, "43m");
    setEnv(ENV_GATEWAY_CLUSTER_NAME, "envCluster");
    setEnv(ENV_GATEWAY_CLUSTER_MEMBER_ID, "envMember");
    setEnv(ENV_GATEWAY_CLUSTER_HOST, "envHost");
    setEnv(ENV_GATEWAY_CLUSTER_PORT, "12345");
    setEnv(ENV_GATEWAY_MONITORING_ENABLED, "true");
    setEnv(ENV_GATEWAY_MONITORING_HOST, "monitorHost");
    setEnv(ENV_GATEWAY_MONITORING_PORT, "231");
    setEnv(ENV_GATEWAY_SECURITY_ENABLED, String.valueOf(true));
    setEnv(
        ENV_GATEWAY_PRIVATE_KEY_PATH,
        GatewayCfgTest.class
            .getClassLoader()
            .getResource("security/test-server.key.pem")
            .getPath());
    setEnv(
        ENV_GATEWAY_CERTIFICATE_PATH,
        GatewayCfgTest.class
            .getClassLoader()
            .getResource("security/test-chain.cert.pem")
            .getPath());
    setEnv(ENV_GATEWAY_KEEP_ALIVE_INTERVAL, "30s");

    final GatewayCfg expected = new GatewayCfg();
    expected.getNetwork().setHost("zeebe").setPort(5432).setMinKeepAliveInterval("30s");
    expected
        .getCluster()
        .setContactPoint("broker:432")
        .setRequestTimeout("43m")
        .setClusterName("envCluster")
        .setMemberId("envMember")
        .setHost("envHost")
        .setPort(12345);
    expected.getThreads().setManagementThreads(32);
    expected.getMonitoring().setEnabled(true).setHost("monitorHost").setPort(231);
    expected
        .getSecurity()
        .setEnabled(true)
        .setPrivateKeyPath(
            getClass().getClassLoader().getResource("security/test-server.key.pem").getPath())
        .setCertificateChainPath(
            getClass().getClassLoader().getResource("security/test-chain.cert.pem").getPath());

    // when
    final GatewayCfg gatewayCfg = readCustomConfig();

    // then
    Assertions.assertThat(gatewayCfg).isEqualTo(expected);
  }

  private void setEnv(final String key, final String value) {
    environment.put(key, value);
  }

  private GatewayCfg readDefaultConfig() {
    return readConfig(DEFAULT_CFG_FILENAME);
  }

  private GatewayCfg readEmptyConfig() {
    return readConfig(EMPTY_CFG_FILENAME);
  }

  private GatewayCfg readCustomConfig() {
    return readConfig(CUSTOM_CFG_FILENAME);
  }

  private GatewayCfg readConfig(final String filename) {
    try (final InputStream inputStream = GatewayCfgTest.class.getResourceAsStream(filename)) {
      if (inputStream != null) {
        final GatewayCfg gatewayCfg =
            new TestConfigurationFactory()
                .create(null, "zeebe-gateway", filename, GatewayCfg.class);
        gatewayCfg.init(new Environment(environment));
        return gatewayCfg;
      } else {
        throw new AssertionError("Unable to find configuration file: " + filename);
      }
    } catch (final IOException e) {
      throw new AssertionError("Failed to read configuration from file: " + filename, e);
    }
  }
}

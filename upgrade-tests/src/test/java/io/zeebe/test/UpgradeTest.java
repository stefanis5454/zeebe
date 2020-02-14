/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.client.api.response.ActivateJobsResponse;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.test.util.TestUtil;
import io.zeebe.util.VersionUtil;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.assertj.core.util.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class UpgradeTest {

  private static final String CURRENT_VERSION = "current-test";
  private static final String PROCESS_ID = "process";
  private static final String TASK = "task";
  private static String lastVersion = VersionUtil.getPreviousVersion();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ContainerStateRule state = new ContainerStateRule();

  @Rule
  public RuleChain chain =
      RuleChain.outerRule(new Timeout(2, TimeUnit.MINUTES)).around(tmpFolder).around(state);

  @Parameter public String testName;

  @Parameter(1)
  public BpmnModelInstance workflow;

  /**
   * Should make zeebe write records and write to state of the feature being tested (e.g., jobs,
   * messages). The workflow should be left in a waiting state so Zeebe can be restarted and
   * execution can be continued after. Takes the container rule as input and outputs a long which
   * can be used after the upgrade to continue the execution.
   */
  @Parameter(2)
  public Function<ContainerStateRule, Long> beforeUpgrade;

  /**
   * Should continue the instance after the upgrade in a way that will complete the workflow. Takes
   * the container rule and a long (e.g., a key) as input.
   */
  @Parameter(3)
  public BiConsumer<ContainerStateRule, Long> afterUpgrade;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {
            "job",
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent()
                .serviceTask(TASK, t -> t.zeebeTaskType(TASK))
                .endEvent()
                .done(),
            (Function<ContainerStateRule, Long>)
                (ContainerStateRule state) -> {
                  final ActivateJobsResponse jobsResponse =
                      state
                          .client()
                          .newActivateJobsCommand()
                          .jobType(TASK)
                          .maxJobsToActivate(1)
                          .send()
                          .join();

                  TestUtil.waitUntil(() -> state.findElementInState(TASK, "ACTIVATED"));
                  return jobsResponse.getJobs().get(0).getKey();
                },
            (BiConsumer<ContainerStateRule, Long>)
                (ContainerStateRule state, Long key) ->
                    state.client().newCompleteCommand(key).send().join()
          },
        });
  }

  @Test
  public void shouldReceiveSbeRecords() {
    // given
    state.startBrokerStandaloneGateway(CURRENT_VERSION, tmpFolder.getRoot().getPath(), lastVersion);

    state
        .client()
        .newDeployCommand()
        .addWorkflowModel(workflow, PROCESS_ID + ".bpmn")
        .send()
        .join();
    state
        .client()
        .newCreateInstanceCommand()
        .bpmnProcessId(PROCESS_ID)
        .latestVersion()
        .send()
        .join();

    // when
    final long key = beforeUpgrade.apply(state);

    // then
    afterUpgrade.accept(state, key);
    TestUtil.waitUntil(() -> state.findElementInState(PROCESS_ID, "ELEMENT_COMPLETED"));
  }

  @Test
  public void shouldRestoreFromOldSnapshot() {
    upgradeZeebe(false);
  }

  @Test
  public void shouldReprocessOldRecords() {
    upgradeZeebe(true);
  }

  private void upgradeZeebe(final boolean deleteSnapshot) {
    // given
    state.startBrokerEmbeddedGateway(lastVersion, tmpFolder.getRoot().getPath());

    state
        .client()
        .newDeployCommand()
        .addWorkflowModel(workflow, PROCESS_ID + ".bpmn")
        .send()
        .join();
    state
        .client()
        .newCreateInstanceCommand()
        .bpmnProcessId(PROCESS_ID)
        .latestVersion()
        .send()
        .join();

    final Long key = beforeUpgrade.apply(state);

    // when
    state.close();
    final File snapshot = new File(tmpFolder.getRoot(), "raft-partition/partitions/1/snapshots");
    assertThat(snapshot.list()).isNotEmpty();

    if (deleteSnapshot) {
      Files.delete(snapshot);
    }

    // then
    state.startBrokerEmbeddedGateway(CURRENT_VERSION, tmpFolder.getRoot().getPath());
    afterUpgrade.accept(state, key);

    TestUtil.waitUntil(() -> state.findElementInState(PROCESS_ID, "ELEMENT_COMPLETED"));
  }
}

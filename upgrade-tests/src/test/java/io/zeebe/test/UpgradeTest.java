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
import java.util.concurrent.TimeUnit;
import org.assertj.core.util.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class UpgradeTest {

  private static final String CURRENT_VERSION = "current-test";
  private static final String PROCESS_ID = "process";
  private static final String TASK = "task";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess(PROCESS_ID)
          .startEvent()
          .serviceTask(TASK, t -> t.zeebeTaskType(TASK))
          .endEvent()
          .done();
  private static String lastVersion = VersionUtil.getPreviousVersion();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ContainerStateRule state = new ContainerStateRule();

  @Rule
  public RuleChain chain =
      RuleChain.outerRule(new Timeout(2, TimeUnit.MINUTES)).around(tmpFolder).around(state);

  @Test
  public void shouldReceiveOlderJobRecords() {
    // given
    state.startBrokerStandaloneGateway(CURRENT_VERSION, tmpFolder.getRoot().getPath(), lastVersion);

    // when
    state
        .client()
        .newDeployCommand()
        .addWorkflowModel(WORKFLOW, PROCESS_ID + ".bpmn")
        .send()
        .join();
    state
        .client()
        .newCreateInstanceCommand()
        .bpmnProcessId(PROCESS_ID)
        .latestVersion()
        .send()
        .join();

    final ActivateJobsResponse jobsResponse =
        state.client().newActivateJobsCommand().jobType(TASK).maxJobsToActivate(1).send().join();

    state.client().newCompleteCommand(jobsResponse.getJobs().get(0).getKey()).send().join();

    // then
    TestUtil.waitUntil(() -> state.findElementInState(PROCESS_ID, "ELEMENT_COMPLETED"));
  }

  @Test
  public void shouldLoadOlderJobState() {
    upgradeJobWorkflow(false);
  }

  @Test
  public void shouldReprocessOlderJobRecords() {
    upgradeJobWorkflow(true);
  }

  private void upgradeJobWorkflow(final boolean deleteSnapshot) {
    // given
    state.startBrokerEmbeddedGateway(lastVersion, tmpFolder.getRoot().getPath());

    state
        .client()
        .newDeployCommand()
        .addWorkflowModel(WORKFLOW, PROCESS_ID + ".bpmn")
        .send()
        .join();
    state
        .client()
        .newCreateInstanceCommand()
        .bpmnProcessId(PROCESS_ID)
        .latestVersion()
        .send()
        .join();

    final ActivateJobsResponse jobsResponse =
        state.client().newActivateJobsCommand().jobType(TASK).maxJobsToActivate(1).send().join();

    TestUtil.waitUntil(() -> state.findElementInState(TASK, "ACTIVATED"));

    // when
    state.close();
    if (deleteSnapshot) {
      final File snapshot = new File(tmpFolder.getRoot(), "raft-partition/partitions/1/snapshots");
      assertThat(snapshot.list()).isNotEmpty();
      Files.delete(snapshot);
    }

    state.startBrokerEmbeddedGateway(CURRENT_VERSION, tmpFolder.getRoot().getPath());
    state.client().newCompleteCommand(jobsResponse.getJobs().get(0).getKey()).send().join();

    // then
    TestUtil.waitUntil(() -> state.findElementInState(PROCESS_ID, "ELEMENT_COMPLETED"));
  }
}

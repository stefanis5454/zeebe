/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.zeebe.broker.system.partitions.RaftMessagingService;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class AtomixRaftMessagingService implements RaftMessagingService {
  private final ClusterCommunicationService communicationService;
  private final ClusterMembershipService clusterMembershipService;
  private final Set<MemberId> otherMembers;

  public AtomixRaftMessagingService(
      final ClusterCommunicationService communicationService,
      final ClusterMembershipService clusterMembershipService,
      final Collection<MemberId> members) {
    this.communicationService = communicationService;
    this.clusterMembershipService = clusterMembershipService;
    this.otherMembers = getOtherMemberIds(clusterMembershipService, members);
  }

  @Override
  public void subscribe(
      final String subject, final Consumer<ByteBuffer> consumer, final Executor executor) {
    communicationService.subscribe(subject, consumer, executor);
  }

  @Override
  public void broadcast(final String subject, final ByteBuffer payload) {
    final var reachableMembers =
        otherMembers.stream()
            .filter(m -> clusterMembershipService.getMember(m).isReachable())
            .collect(Collectors.toUnmodifiableSet());

    communicationService.multicast(subject, payload, reachableMembers);
  }

  @Override
  public void unsubscribe(final String subject) {
    communicationService.unsubscribe(subject);
  }

  private Set<MemberId> getOtherMemberIds(
      final ClusterMembershipService clusterMembershipService,
      final Collection<MemberId> raftMembers) {
    final var localMemberId = clusterMembershipService.getLocalMember().id();
    final var eligibleMembers = new HashSet<>(raftMembers);
    eligibleMembers.remove(localMemberId);

    return Collections.unmodifiableSet(eligibleMembers);
  }
}

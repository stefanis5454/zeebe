/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.storage.atomix;

import io.atomix.protocols.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.index.JournalIndex;
import io.atomix.storage.journal.index.Position;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class ZeebeIndexBridge implements JournalIndex, ZeebeIndexMapping {

  private static final int DENSITY = 100;

  private final ConcurrentNavigableMap<Long, Long> positionIndexMapping =
      new ConcurrentSkipListMap<>();
  private final ConcurrentNavigableMap<Long, Long> indexPositionMapping =
      new ConcurrentSkipListMap<>();
  // atomix positions
  private final ConcurrentNavigableMap<Long, Integer> positions = new ConcurrentSkipListMap<>();

  @Override
  public void index(final Indexed indexedEntry, final int position) {
    final var index = indexedEntry.index();
    if (index % DENSITY == 0) {
      if (indexedEntry.type() == ZeebeEntry.class) {
        final ZeebeEntry zeebeEntry = (ZeebeEntry) indexedEntry.entry();
        final var lowestPosition = zeebeEntry.lowestPosition();
        positionIndexMapping.put(lowestPosition, index);
        indexPositionMapping.put(index, lowestPosition);
      }
      positions.put(index, position);
    }
  }

  @Override
  public long lookupPosition(final long position) {
    final long startTime = System.currentTimeMillis();

    var index = positionIndexMapping.getOrDefault(position, -1L);

    if (index == -1) {
      final var lowerEntry = positionIndexMapping.lowerEntry(position);
      if (lowerEntry != null) {
        index = lowerEntry.getValue();
      }
    }

    final long endTime = System.currentTimeMillis();
    io.zeebe.logstreams.impl.Loggers.LOGSTREAMS_LOGGER.info(
        "Finding position {} in map took: {} ms ", position, endTime - startTime);

    return index;
  }

  @Override
  public Position lookup(final long index) {
    final Map.Entry<Long, Integer> entry = positions.floorEntry(index);
    return entry != null ? new Position(entry.getKey(), entry.getValue()) : null;
  }

  @Override
  public void truncate(final long index) {
    final var lowerEntry = indexPositionMapping.lowerEntry(index);

    final var lowerIndex = lowerEntry.getKey();
    final var lowerPosition = lowerEntry.getValue();

    indexPositionMapping.tailMap(lowerIndex).clear();
    positionIndexMapping.tailMap(lowerPosition).clear();

    // clean up map
    //
    //      final var positionToIndexMapping = getPositionToIndexMapping();
    //      if (!positionToIndexMapping.isEmpty()) {
    //        final var newPositionToIndexMap =
    //            positionToIndexMapping.subMap(
    //                positionToIndexMapping.higherKey(position),
    //                true,
    //                positionToIndexMapping.lastKey(),
    //                true);
    //        positionToIndexMappingRef.set(newPositionToIndexMap);
    //      }

    //    sparseJournalIndex.truncate(index);
    positions.tailMap(index, false).clear();
  }
}

package io.zeebe.logstreams.storage.atomix;

public interface ZeebeIndexMapping {

  /**
   * Takes a position and returns the corresponding index, where this entry with the given position is stored.
   *
   * @param position
   * @return
   */
  long lookupPosition(long position);
}

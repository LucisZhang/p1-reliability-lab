package com.p1.reliability.cdc;

import java.io.Serializable;

/** A Path B source record: either one decoded order change or one quarantined DLQ payload. */
public final class BrokerRecord implements Serializable {
  private static final long serialVersionUID = 1L;

  public OrderChange change;
  public String deadLetterJson;

  public BrokerRecord() {}

  private BrokerRecord(OrderChange change, String deadLetterJson) {
    this.change = change;
    this.deadLetterJson = deadLetterJson;
  }

  public static BrokerRecord decoded(OrderChange change) {
    return new BrokerRecord(change, null);
  }

  public static BrokerRecord deadLetter(String deadLetterJson) {
    return new BrokerRecord(null, deadLetterJson);
  }

  public boolean isDecoded() {
    return change != null;
  }

  public boolean isDeadLetter() {
    return deadLetterJson != null;
  }
}

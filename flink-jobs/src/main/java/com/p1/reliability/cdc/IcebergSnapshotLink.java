package com.p1.reliability.cdc;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;

public final class IcebergSnapshotLink {
  private IcebergSnapshotLink() {}

  public static void main(String[] args) {
    JobConfig config = JobConfig.fromArgs(args);
    Catalog catalog = IcebergTables.loadCatalog(config);
    Table current = catalog.loadTable(IcebergTables.currentIdentifier(config));
    Table changelog = catalog.loadTable(IcebergTables.changelogIdentifier(config));
    System.out.println(
        "{\"orders_current\":"
            + snapshotId(current)
            + ",\"orders_changelog\":"
            + snapshotId(changelog)
            + "}");
  }

  private static long snapshotId(Table table) {
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      throw new IllegalStateException("Table has no current snapshot: " + table.name());
    }
    return snapshot.snapshotId();
  }
}

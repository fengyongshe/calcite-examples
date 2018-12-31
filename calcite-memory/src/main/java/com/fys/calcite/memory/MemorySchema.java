package com.fys.calcite.memory;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import com.fys.calcite.memory.MemoryData.Database;

import java.util.HashMap;
import java.util.Map;

public class MemorySchema extends AbstractSchema {

  private String dbName;

  public MemorySchema(String dbName) {
    this.dbName = dbName;
  }

  @Override
  public Map<String,Table> getTableMap() {
    Map<String,Table> tables = new HashMap<>();
    Database database = MemoryData.MAP.get(this.dbName);
    if(database == null) {
      return tables;
    }
    for(MemoryData.Table table : database.tables) {
      tables.put(table.tableName, new MemoryTable(table));
    }
    return tables;
  }

}

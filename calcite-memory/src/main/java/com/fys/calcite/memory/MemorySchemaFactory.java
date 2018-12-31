package com.fys.calcite.memory;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class MemorySchemaFactory implements SchemaFactory {

  @Override
  public Schema create(SchemaPlus parentSchema, String name,
                       Map<String, Object> operand) {
    System.out.println("param1:" + operand.get("param1"));
    System.out.println("parma2:" + operand.get("param2"));
    System.out.println("Get Database" + name);
    return new MemorySchema(name);
  }

}

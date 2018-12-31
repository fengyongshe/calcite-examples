package com.fys.calcite.csv;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import java.util.ArrayList;
import java.util.List;

public abstract class CsvTable extends AbstractTable {

  protected final Source source;
  protected final RelProtoDataType protoRowType;
  protected List<CsvFieldType> fieldTypes;

  CsvTable(Source source, RelProtoDataType protoRowType) {
    this.source = source;
    this.protoRowType = protoRowType;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (fieldTypes == null ) {
      fieldTypes = new ArrayList<CsvFieldType>();
      return CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
          fieldTypes);
    } else {
      return CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
          null);
    }
  }

  public enum Flavor {
    SCANNABLE, FILTERTABLE, TRANSLATABLE
  }
}

package com.fys.calcite.file;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.Source;

import java.util.List;
import java.util.Map;

public class FileTable extends AbstractQueryableTable implements TranslatableTable  {

  private final RelProtoDataType protoRowType;
  private FileReader reader;
  private FileRowConverter converter;

  private FileTable(Source source, String selector, Integer index,
                    RelProtoDataType protoRowType,
                    List<Map<String,Object>> fieldConfigs) throws Exception {
    super(Object[].class);
    this.protoRowType = protoRowType;
    this.reader = new FileReader(source, selector, index);
    this.converter = new FileRowConverter(this.reader, fieldConfigs);
  }

  static FileTable create(Source source, Map<String,Object> tableDef) throws Exception {
    List<Map<String,Object>> fieldConfigs =
        (List<Map<String,Object>>) tableDef.get("fields");
    String selector = (String) tableDef.get("selector");
    Integer index = (Integer) tableDef.get("index");
    return new FileTable(source,selector, index, null, fieldConfigs);
  }

  public String toString() {
    return "FileTable";
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      public Enumerator<T> enumerator() {
        try {
          FileEnumerator enumerator =
              new FileEnumerator(reader.iterator(), converter);
          //noinspection unchecked
          return (Enumerator<T>) enumerator;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  /** Returns an enumerable over a given projection of the fields. */
  public Enumerable<Object> project(final int[] fields) {
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        try {
          return new FileEnumerator(reader.iterator(), converter, fields);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public RelNode toRel(RelOptTable.ToRelContext context,
                       RelOptTable relOptTable) {
    return new EnumerableTableScan(context.getCluster(),
        context.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
        relOptTable, (Class) getElementType());
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    return this.converter.getRowType((JavaTypeFactory) typeFactory);
  }


}

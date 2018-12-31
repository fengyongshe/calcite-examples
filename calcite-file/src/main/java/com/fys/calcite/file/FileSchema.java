package com.fys.calcite.file;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.csv.CsvFilterableTable;
import org.apache.calcite.adapter.csv.JsonTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

public class FileSchema extends AbstractSchema {

  private final ImmutableList<Map<String,Object>> tables;
  private final File baseDirectory;

  FileSchema(SchemaPlus parentSchema, String name, File baseDirectory ,
             List<Map<String,Object>> tables) {
    this.tables = ImmutableList.copyOf(tables);
    this.baseDirectory = baseDirectory;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string. */
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null. */
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String,Table> builder = ImmutableMap.builder();
    for( Map<String,Object> tableDef: this.tables) {
      String tableName = (String) tableDef.get("name");
      try {
        addTable(builder, tableDef);
      } catch (MalformedURLException e) {
        throw new RuntimeException("Unable to instantiate table for:" +
            tableName);
      }
    }
    final Source baseSource = Sources.of(baseDirectory);
    File[] files = baseDirectory.listFiles(
        new FilenameFilter() {
          public boolean accept(File dir, String name) {
            final String nameSansGz = trim(name, ".gz");
            return nameSansGz.endsWith(".csv")
                || nameSansGz.endsWith(".json");
          }
        }
    );
    if(files == null) {
      System.out.println("Directory " + baseDirectory + " not found");
      files = new File[0];
    }
    for (File file : files) {
      Source source = Sources.of(file);
      Source sourceSansGz = source.trim(".gz");
      final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
      if(sourceSansGz != null) {
        JsonTable table = new JsonTable(source);
        builder.put(sourceSansJson.relative(baseSource).path(), table);
        continue;
      }
      final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
      if(sourceSansCsv != null) {
        addTable(builder, source, sourceSansCsv.relative(baseSource).path(),
            null);
      }
    }

    return builder.build();
  }

  public boolean addTable(ImmutableMap.Builder<String,Table> builder,
                          Map<String,Object> tableDef) throws MalformedURLException {
    final String tableName = (String) tableDef.get("name");
    final String url = (String) tableDef.get("url");
    final Source source0 = Sources.url(url);
    final Source source;
    if(baseDirectory == null) {
      source = source0;
    } else {
      source = Sources.of(baseDirectory).append(source0);
    }
    return addTable(builder, source, tableName, tableDef);
  }

  private boolean addTable(ImmutableMap.Builder<String,Table> builder,
                           Source source,
                           String tableName,
                           Map<String, Object> tableDef) {
    final Source sourceSansGz = source.trim(".gz");
    final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
    if(sourceSansJson != null) {
      JsonTable table = new JsonTable(source);
      builder.put(Util.first(tableName, sourceSansJson.path()), table);
      return true;
    }
    final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
    if (sourceSansCsv != null) {
      final Table table = new CsvFilterableTable(source, null);
      builder.put(Util.first(tableName, sourceSansCsv.path()), table);
      return true;
    }
    if(tableDef != null) {
      try {
        FileTable table = FileTable.create(source, tableDef);
        builder.put(Util.first(tableName, source.path()), table);
        return true;
      } catch (Exception e) {
        throw new RuntimeException("Unable to instantiate table for: " + tableName);
      }
    }
    return false;
  }

}

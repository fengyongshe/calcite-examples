package com.fys.calcite.file;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.List;
import java.util.Map;

public class FileSchemaFactory implements SchemaFactory {
  public Schema create(SchemaPlus parentSchema, String name,
                       Map<String, Object> operand) {
    List<Map<String,Object>> tables = (List) operand.get("tables");
    final File baseDirectory =
        (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    File directoryFile = baseDirectory;
    final String directory = (String) operand.get("directory");
    if(baseDirectory != null && directory != null) {
      directoryFile = new File(directory);
      if(!directoryFile.isAbsolute()) {
        directoryFile = new File(baseDirectory,directory);
      }
    }
    return new FileSchema(parentSchema,name, directoryFile, tables);
  }
}

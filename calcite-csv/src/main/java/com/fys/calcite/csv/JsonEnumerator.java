package com.fys.calcite.csv;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.util.Source;

import java.io.IOException;
import java.util.List;

public class JsonEnumerator implements Enumerator<Object[]> {

  private final Enumerator<Object> enumerator;

  JsonEnumerator(Source source) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
      mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      List<Object> list;
      if(source.protocol().equals("file")) {
        list = mapper.readValue(source.file(), List.class);
      } else {
        list = mapper.readValue(source.url(), List.class);
      }
      enumerator = Linq4j.enumerator(list);
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object[] current() {
    return new Object[]{
        enumerator.current()
    };
  }

  @Override
  public boolean moveNext() {
    return enumerator.moveNext();
  }

  @Override
  public void reset() {
    enumerator.reset();
  }

  @Override
  public void close() {
    try {
      enumerator.close();
    } catch (Exception e) {
      throw new RuntimeException("Error closing JSON Reader",e);
    }
  }
}

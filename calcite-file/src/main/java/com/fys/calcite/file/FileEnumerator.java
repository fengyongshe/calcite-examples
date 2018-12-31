package com.fys.calcite.file;

import org.apache.calcite.linq4j.Enumerator;
import org.jsoup.select.Elements;

import java.util.Iterator;

public class FileEnumerator implements Enumerator<Object> {

  private final Iterator<Elements> iterator;
  private final FileRowConverter converter;
  private final int[] fields;
  private Object current;

  FileEnumerator(Iterator<Elements> iterator, FileRowConverter converter) {
    this(iterator, converter, identityList(converter.width()));
  }

  FileEnumerator(Iterator<Elements> iterator, FileRowConverter converter,
                 int[] fields) {
    this.iterator = iterator;
    this.converter = converter;
    this.fields = fields;
  }

  public Object current() {
    if (current == null) {
      this.moveNext();
    }
    return current;
  }

  public boolean moveNext() {

    try {
      if(this.iterator.hasNext()) {
        final Elements row = this.iterator.next();
        current = this.converter.toRow(row,this.fields);
        return true;
      } else {
        current = null;
        return false;
      }
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }
  // required by linq4j Enumerator interface
  public void reset() {
    throw new UnsupportedOperationException();
  }

  // required by linq4j Enumerator interface
  public void close() {
  }

  /** Returns an array of integers {0, ..., n - 1}. */
  private static int[] identityList(int n) {
    int[] integers = new int[n];

    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }

    return integers;
  }
}

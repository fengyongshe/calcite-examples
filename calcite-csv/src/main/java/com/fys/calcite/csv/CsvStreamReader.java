package com.fys.calcite.csv;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import org.apache.calcite.util.Source;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.Queue;

public class CsvStreamReader extends CSVReader implements Closeable {

  protected CSVParser parser;
  protected int skipLines;
  protected Tailer tailer;
  protected Queue<String> contentQueue;

  public static final int DEFAULT_SKIP_LINES = 0;
  public static final long DEFAULT_MONITOR_DELAY = 2000;

  CsvStreamReader(Source source) {
    this(source,
        CSVParser.DEFAULT_SEPARATOR,
        CSVParser.DEFAULT_QUOTE_CHARACTER,
        CSVParser.DEFAULT_ESCAPE_CHARACTER,
        DEFAULT_SKIP_LINES,
        CSVParser.DEFAULT_STRICT_QUOTES,
        CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);
  }

  private CsvStreamReader(Source source, char separator, char quoteChar,
                          char escape, int line, boolean strictQuotes,
                          boolean ignoreLeadingWhiteSpace) {
    super(new StringReader("")); // dummy call to base constructor
    contentQueue = new ArrayDeque<>();
    TailerListener listener = new CsvContentListener(contentQueue);
    tailer = Tailer.create(source.file(), listener, DEFAULT_MONITOR_DELAY,
        false, true, 4096);
    this.parser = new CSVParser(separator, quoteChar, escape, strictQuotes,
        ignoreLeadingWhiteSpace);
    this.skipLines = line;
    try {
      // wait for tailer to capture data
      Thread.sleep(DEFAULT_MONITOR_DELAY);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public String[] readNext() throws IOException {
    String[] result = null;
    do {
      String nextLine = getNextLine();
      if(nextLine == null) {
        return null;
      }
      String[] r = parser.parseLineMulti(nextLine);
      if(r.length > 0) {
        if(result == null) {
          result = r;
        } else {
          String[] t = new String[result.length + r.length];
          System.arraycopy(result, 0, t, 0, result.length);
          System.arraycopy(r, 0, t, result.length, r.length);
          result = t;
        }
      }
    } while(parser.isPending());
    return  result;
  }

  private String getNextLine() throws IOException {
    return contentQueue.poll();
  }

  public void close() throws IOException {
  }

  private static class CsvContentListener extends TailerListenerAdapter {
    final Queue<String> contentQueue;

    CsvContentListener(Queue<String> contentQueue) {
      this.contentQueue = contentQueue;
    }

    @Override public void handle(String line) {
      this.contentQueue.add(line);
    }
  }
}

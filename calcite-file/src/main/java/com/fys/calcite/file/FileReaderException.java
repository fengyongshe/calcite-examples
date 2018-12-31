package com.fys.calcite.file;

public class FileReaderException extends Exception  {

  FileReaderException(String message) {
    super(message);
  }

  FileReaderException(String message, Throwable e) {
    super(message,e);
  }
}

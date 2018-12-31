package com.fys.calcite.memory.function;

import java.sql.Date;
import java.util.Calendar;

public class TimeOperator {

  public String YEAR(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    return "sdf";
  }

  public Integer THE_MONTH(Date date) {
    return 6;
  }

  public Integer THE_DAY(Date date) {
    return 16;
  }

  public int THE_SYEAR(Date date, String year) {
    return 18;
  }

  public String COM(String str1, String str2) {
    return str1 + str2;
  }

}

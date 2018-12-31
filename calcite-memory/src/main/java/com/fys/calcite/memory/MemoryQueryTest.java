package com.fys.calcite.memory;

import com.fys.calcite.memory.function.TimeOperator;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class MemoryQueryTest {

  public static void main(String[] args) {
    try {

      Class.forName("org.apache.calcite.jdbc.Driver");
      Properties info = new Properties();
      Connection connection =
          DriverManager.getConnection("jdbc:calcite:model=/root/calcite-examples/calcite-memory/src/main/resources/School.json",info);
      CalciteConnection calciteCon = connection.unwrap(CalciteConnection.class);
      calciteCon.getRootSchema().add("YEAR",
          ScalarFunctionImpl.create(TimeOperator.class.getMethod("YEAR", Date.class)));
      calciteCon.getRootSchema().add("COM",
          ScalarFunctionImpl.create(TimeOperator.class.getMethod("COM", String.class, String.class)));

      ResultSet result = connection.getMetaData().getTables(null,null,null,null);
      while(result.next()) {
        System.out.println("Catalog:" + result.getString(1) + ",Database : "
            + result.getString(2) + ", Table: "+ result.getString(3) );
      }
      result.close();

      result = connection.getMetaData().getColumns(null,null,"Student",null);
      while(result.next()) {
        System.out.println("name : " + result.getString(4) + ", type : " + result.getString(5) + ", typename : " + result.getString(6));
      }
      result.close();

      Statement st = connection.createStatement();
      result  = st.executeQuery("select S.\"id\", SUM(S.\"classId\") from \"Student\" as S group by S.\"id\"");
      while(result.next()) {
        System.out.println(result.getString(1) + "\t" + result.getString(2));
      }
      result.close();
      connection.close();

    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }

}

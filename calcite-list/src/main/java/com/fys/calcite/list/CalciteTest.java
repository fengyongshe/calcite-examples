package com.fys.calcite.list;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class CalciteTest {

  public static void main(String[] args) throws Exception {

    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:" ,info);
    CalciteConnection calciteCon = (CalciteConnection) connection;
    SchemaPlus rootSchema = calciteCon.getRootSchema();

    ReflectiveSchema hrs = new ReflectiveSchema(new JavaHrSchema());
    rootSchema.add("hr", hrs);

    Statement statement = calciteCon.createStatement();
    ResultSet rs = statement.executeQuery("select * from hr.emps as e join hr.depts as d on e.deptno = d.deptno");
    System.out.println("Result MetaData:" + rs.getMetaData().toString());
    System.out.println("colume count:" + rs.getMetaData().getColumnCount());
    while(rs.next()) {
      for (int i = 1; i < rs.getMetaData().getColumnCount(); i++) {
        System.out.print(rs.getObject(i).toString() + " ");
      }
    }

    rs.close();
    statement.close();
    connection.close();

  }
}

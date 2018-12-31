package com.fys.calcite.list;

public class JavaHrSchema {

  public final Employee[] emps = {
      new Employee(100, "blueJoe", 1),
      new Employee(200, "waz",2),
      new Employee(1500, "wxa", 1)
  };

  public final Department[] depts = { new Department(1, "dev"),
      new Department(2, "market") };

  public static class Employee {
    public final int empid;
    public final String name;
    public final int deptno;

    public Employee(int empid, String name, int deptno) {
      this.empid = empid;
      this.name = name;
      this.deptno = deptno;
    }

  }

  public static class Department {
    public final String name;
    public final int deptno;

    public Department(int deptno, String name) {
      this.name = name;
      this.deptno = deptno;
    }
  }
}

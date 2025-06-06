package com.packt.sfjd.ch8;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class UDFExampleUpdated {

    public static void main(String[] args) {
        // Build a Spark Session
        SparkSession sparkSession = SparkSession
            .builder()
            .master("local")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .appName("UDF Example")
            .getOrCreate();
            
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN); 
        
        // Read the CSV data
        Dataset<Row> emp_ds = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("src/main/resources/Employee.txt");    
        
        UDF2 calcDays = new CalcDaysUDF();
        // Registering the UDFs in Spark Session created above      
        sparkSession.udf().register("calcDays", calcDays, DataTypes.LongType);
        
        emp_ds.createOrReplaceTempView("emp_ds");
        
        emp_ds.printSchema();
        emp_ds.show();
        
        sparkSession.sql("select calcDays(hiredate,'dd-MM-yyyy') from emp_ds").show();   
        
        // Instantiate UDAF
        AverageUDAF calcAvg = new AverageUDAF();
        // Register UDAF to SparkSession
        sparkSession.udf().register("calAvg", calcAvg);
        // Use UDAF
        sparkSession.sql("select deptno,calAvg(salary) from emp_ds group by deptno").show(); 
       
        // TypeSafe UDAF example
        TypeSafeUDAF typeSafeUDAF = new TypeSafeUDAF();
        
        Dataset<Employee> emf = emp_ds.as(Encoders.bean(Employee.class));
        emf.printSchema();
        emf.show();
        
        TypedColumn<Employee, Double> averageSalary = typeSafeUDAF.toColumn().name("averageTypeSafe");
        Dataset<Double> result = emf.select(averageSalary);
        result.show();
    }
}
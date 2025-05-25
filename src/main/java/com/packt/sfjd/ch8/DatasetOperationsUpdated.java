package com.packt.sfjd.ch8;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col; 

public class DatasetOperationsUpdated {

    public static void main(String[] args) throws AnalysisException {
        // Set a temporary directory for Spark warehouse
        System.setProperty("hadoop.home.dir", "/tmp/hadoop");
        
        // Build a Spark Session    
        SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .appName("DatasetOperations")
            //.enableHiveSupport()
            .getOrCreate();
            
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN); 
        
        // Create a RDD
        JavaRDD<String> deptRDD = sparkSession.sparkContext()
                .textFile("src/main/resources/dept.txt", 1)
                .toJavaRDD();

        // Convert the RDD to RDD<Rows>
        JavaRDD<Row> deptRows = deptRDD.filter(str-> !str.contains("deptno")).map(
            (Function<String, Row>) rowString -> {
                String[] cols = rowString.split(",");
                return RowFactory.create(cols[0].trim(), cols[1].trim(), cols[2].trim());
            }
        );
          
        // Create schema         
        String[] schemaArr = deptRDD.first().split(",");
        List<StructField> structFieldList = new ArrayList<>();
        for (String fieldName : schemaArr) {
            StructField structField = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            structFieldList.add(structField);
        }
        StructType schema = DataTypes.createStructType(structFieldList);
          
        Dataset<Row> deptDf = sparkSession.createDataFrame(deptRows, schema);
        deptDf.printSchema();
        deptDf.show();
          
        deptDf.createOrReplaceTempView("dept");    
          
        Dataset<Row> result = sparkSession.sql("select loc,count(loc) from dept where deptno > 10 group by loc");
        result.show();
          
        deptDf.createGlobalTempView("dept_global_view");
          
        sparkSession.newSession().sql("SELECT deptno,dname,loc, rank() OVER (PARTITION BY loc ORDER BY deptno) FROM global_temp.dept_global_view").show();
         
        // Write to different formats
        deptDf.write().mode(SaveMode.Overwrite).json("src/main/resources/output/dept");
        deptDf.write().mode(SaveMode.Overwrite).format("csv").save("src/main/resources/output/deptText");
        deptDf.write().mode("overwrite").format("csv").save("src/main/resources/output/deptText");
         
        // Read the CSV data
        Dataset<Row> emp_ds = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("src/main/resources/Employee.txt");    
             
        emp_ds.printSchema();
        emp_ds.show();
             
        emp_ds.select("empName", "empId").show();
            
        emp_ds.select(col("empName").as("Employee Name"), col("empId").cast(DataTypes.IntegerType).as("Employee Id")).show();
            
        emp_ds.sort(col("empId").asc()).filter(col("salary").gt("2500"));
            
        emp_ds.select("job").groupBy(col("job")).count().show();
            
        emp_ds.as("A").join(deptDf.as("B"), emp_ds.col("deptno").equalTo(deptDf.col("deptno")), "left")
            .select("A.empId", "A.empName", "A.job", "A.manager", "A.hiredate", "A.salary", "A.comm", "A.deptno", "B.dname", "B.loc")
            .show();
            
        emp_ds.join(deptDf, emp_ds.col("deptno").equalTo(deptDf.col("deptno")), "right").show();            
        
        emp_ds.join(deptDf, emp_ds.col("deptno").equalTo(deptDf.col("deptno")), "right").explain();
             
        sparkSession.sql("show functions").show(false);
        sparkSession.sql("DESCRIBE FUNCTION add_months").show(false);
        sparkSession.sql("DESCRIBE FUNCTION EXTENDED add_months").show(false);
        
        // Stop the SparkSession
        sparkSession.stop();
    }
}
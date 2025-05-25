# Spark 3.x Upgrade TODO List

This document tracks the progress of upgrading examples from Spark 2.x to Spark 3.x.

## Status Legend
- [Pending] - Not yet upgraded
- [Finished] - Successfully upgraded to Spark 3.x
- [Blocked] - Cannot be upgraded due to compatibility issues

## Chapter 2 - Java 8 Examples
- [Finished] src/main/java/com/packt/sfjd/ch2/AInnerClassVsLambda.java
- [Finished] src/main/java/com/packt/sfjd/ch2/Car.java
- [Finished] src/main/java/com/packt/sfjd/ch2/ClosureDemo.java
- [Finished] src/main/java/com/packt/sfjd/ch2/ClosureExample.java
- [Finished] src/main/java/com/packt/sfjd/ch2/CollectorsExamples.java
- [Finished] src/main/java/com/packt/sfjd/ch2/CreateStreamExample.java
- [Finished] src/main/java/com/packt/sfjd/ch2/CreateStreamExampleSimple.java
- [Finished] src/main/java/com/packt/sfjd/ch2/Interface1.java
- [Finished] src/main/java/com/packt/sfjd/ch2/Interface2.java
- [Finished] src/main/java/com/packt/sfjd/ch2/InterfaceImpl.java
- [Finished] src/main/java/com/packt/sfjd/ch2/InterfaceImplTest.java
- [Finished] src/main/java/com/packt/sfjd/ch2/IntermediateOpExample.java
- [Finished] src/main/java/com/packt/sfjd/ch2/LambdaExamples.java
- [Finished] src/main/java/com/packt/sfjd/ch2/LexicalScoping.java
- [Finished] src/main/java/com/packt/sfjd/ch2/MethodReferenceExample.java
- [Finished] src/main/java/com/packt/sfjd/ch2/MyFileNameFilter.java
- [Finished] src/main/java/com/packt/sfjd/ch2/MyFilterImpl.java
- [Finished] src/main/java/com/packt/sfjd/ch2/MyInterface.java
- [Finished] src/main/java/com/packt/sfjd/ch2/MyInterfaceDemo.java
- [Finished] src/main/java/com/packt/sfjd/ch2/MyInterfaceImpl.java
- [Finished] src/main/java/com/packt/sfjd/ch2/ShortCircuitOperationExample.java
- [Finished] src/main/java/com/packt/sfjd/ch2/TerminalOpExample.java
- [Finished] src/main/java/com/packt/sfjd/ch2/WordCountInJava.java
- [Finished] src/main/java/com/packt/sfjd/ch2/generics/FirstExample.java
- [Finished] src/main/java/com/packt/sfjd/ch2/generics/MyGeneric.java
- [Finished] src/main/java/com/packt/sfjd/ch2/generics/MyGenericsDemo.java

## Chapter 4 - RDD Examples
- [Finished] src/main/java/com/packt/sfjd/ch4/ActionExamples.java
- [Finished] src/main/java/com/packt/sfjd/ch4/ActionsExamplesOld.java
- [Finished] src/main/java/com/packt/sfjd/ch4/AggeregateExample.java
- [Finished] src/main/java/com/packt/sfjd/ch4/JavaWordCount.java
- [Finished] src/main/java/com/packt/sfjd/ch4/PersistExample.java
- [Finished] src/main/java/com/packt/sfjd/ch4/SparkWordCount.java
- [Finished] src/main/java/com/packt/sfjd/ch4/SparkWordCount_1_7.java
- [Finished] src/main/java/com/packt/sfjd/ch4/WordCount.java
- [Finished] src/main/java/com/packt/sfjd/ch4/transformations/Test.java
- [Finished] src/main/java/com/packt/sfjd/ch4/transformations/TestMain.java
- [Finished] src/main/java/com/packt/sfjd/ch4/transformations/Transformations.java

## Chapter 5 - Data Sources
- [Finished] src/main/java/com/packt/sfjd/ch5/CSVFileOperations.java
- [Finished] src/main/java/com/packt/sfjd/ch5/CassandraExample.java
- [Finished] src/main/java/com/packt/sfjd/ch5/DelimitedFileOperations.java
- [Finished] src/main/java/com/packt/sfjd/ch5/Employee.java
- [Blocked] src/main/java/com/packt/sfjd/ch5/HdfsExample.java
- [Finished] src/main/java/com/packt/sfjd/ch5/JsonFileOperations.java
- [Finished] src/main/java/com/packt/sfjd/ch5/LFSExample.java
- [Finished] src/main/java/com/packt/sfjd/ch5/Movie.java
- [Finished] src/main/java/com/packt/sfjd/ch5/Person.java
- [Finished] src/main/java/com/packt/sfjd/ch5/PersonDetails.java
- [Blocked] src/main/java/com/packt/sfjd/ch5/S3Example.java
- [Finished] src/main/java/com/packt/sfjd/ch5/TextFileOperations.java
- [Blocked] src/main/java/com/packt/sfjd/ch5/XMLFileOperations.java

## Chapter 7 - Advanced RDD Operations
- [Blocked] src/main/java/com/packt/sfjd/ch7/AdvanceActionExamples.java (Created AdvanceActionExamplesUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch7/BroadcastVariable.java (Created BroadcastVariableUpdated.java instead)
- [Finished] src/main/java/com/packt/sfjd/ch7/CustomPartitioner.java
- [Blocked] src/main/java/com/packt/sfjd/ch7/CustomPartitionerExample.java (Created CustomPartitionerExampleUpdated.java instead)
- [Finished] src/main/java/com/packt/sfjd/ch7/ListAccumulator.java
- [Finished] src/main/java/com/packt/sfjd/ch7/MapSideJoinBroadcast.java
- [Finished] src/main/java/com/packt/sfjd/ch7/PartitionIndexInformation.java
- [Blocked] src/main/java/com/packt/sfjd/ch7/Partitioning.java (Created PartitioningUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch7/TestAccumulator.java (Created TestAccumulatorUpdated.java instead)
- [Finished] src/main/java/com/packt/sfjd/ch7/Transformations.java

## Chapter 8 - Spark SQL
- [Finished] src/main/java/com/packt/sfjd/ch8/Average.java
- [Finished] src/main/java/com/packt/sfjd/ch8/AverageUDAF.java
- [Finished] src/main/java/com/packt/sfjd/ch8/CalcDaysUDF.java
- [Finished] src/main/java/com/packt/sfjd/ch8/ContextCreation.java
- [Blocked] src/main/java/com/packt/sfjd/ch8/DatasetOperations.java (Created DatasetOperationsUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch8/DfExample.java (Created DfExampleUpdated.java instead)
- [Finished] src/main/java/com/packt/sfjd/ch8/DsExample.java
- [Finished] src/main/java/com/packt/sfjd/ch8/Employee.java
- [Finished] src/main/java/com/packt/sfjd/ch8/SparkSessionExample.java
- [Blocked] src/main/java/com/packt/sfjd/ch8/SparkSessionHeloWorld.java (Created SparkSessionHelloWorldUpdated.java instead)
- [Finished] src/main/java/com/packt/sfjd/ch8/TypeSafeUDAF.java
- [Blocked] src/main/java/com/packt/sfjd/ch8/UDFExample.java (Created UDFExampleUpdated.java instead)

## Chapter 9 - Spark Streaming
- [Finished] src/main/java/com/packt/sfjd/ch9/Calculator.java
- [Blocked] src/main/java/com/packt/sfjd/ch9/FileStreamingEx.java (Created FileStreamingExUpdated.java instead)
- [Finished] src/main/java/com/packt/sfjd/ch9/FlightDetails.java
- [Blocked] src/main/java/com/packt/sfjd/ch9/KafkaExample.java (Created KafkaExampleUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch9/StateFulProcessingExample.java (Created StateFulProcessingExampleUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch9/StateLessProcessingExample.java (Created StateLessProcessingExampleUpdated.java instead)
- [Finished] src/main/java/com/packt/sfjd/ch9/StructuredStreamingExample.java
- [Finished] src/main/java/com/packt/sfjd/ch9/TweetText.java
- [Blocked] src/main/java/com/packt/sfjd/ch9/WindowBatchInterval.java (Created WindowBatchIntervalUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch9/WordCountRecoverableEx.java (Created WordCountRecoverableExUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch9/WordCountSocketEx.java (Created WordCountSocketExUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch9/WordCountSocketJava8Ex.java (Created WordCountSocketJava8ExUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch9/WordCountSocketStateful.java (Created WordCountSocketStatefulUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch9/WordCountTransformOpEx.java (Created WordCountTransformOpExUpdated.java instead)

## Chapter 10 - MLlib
- [Blocked] src/main/java/com/packt/sfjd/ch10/BikeRentalPrediction.java (Created BikeRentalPredictionUpdated.java instead)
- [Finished] src/main/java/com/packt/sfjd/ch10/Flight.java
- [Finished] src/main/java/com/packt/sfjd/ch10/FlightDelay.java
- [Blocked] src/main/java/com/packt/sfjd/ch10/JavaALSExample.java (Created JavaALSExampleUpdated.java instead)
- [Blocked] src/main/java/com/packt/sfjd/ch10/JavaEstimatorTransformerParamExample.java (Created JavaEstimatorTransformerParamExampleUpdated.java instead)
- [Finished] src/main/java/com/packt/sfjd/ch10/Rating.java

## Chapter 11 - GraphX
- [Pending] src/main/java/com/packt/sfjd/ch11/AbsFunc1.java
- [Pending] src/main/java/com/packt/sfjd/ch11/AbsFunc2.java
- [Pending] src/main/java/com/packt/sfjd/ch11/AbsFunc3.java
- [Pending] src/main/java/com/packt/sfjd/ch11/AbsFunc4.java
- [Pending] src/main/java/com/packt/sfjd/ch11/AbsFunc5.java
- [Pending] src/main/java/com/packt/sfjd/ch11/AbsFunc6.java
- [Pending] src/main/java/com/packt/sfjd/ch11/AbsFunc7.java
- [Pending] src/main/java/com/packt/sfjd/ch11/AbsFunc8.java
- [Pending] src/main/java/com/packt/sfjd/ch11/PropertyGraphExample.java
- [Pending] src/main/java/com/packt/sfjd/ch11/PropertyGraphExampleFromEdges.java
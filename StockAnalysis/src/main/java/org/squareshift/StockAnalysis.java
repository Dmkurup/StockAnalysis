package org.squareshift;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class StockAnalysis {
    static Logger log = LogManager.getRootLogger();
    static String inputPath ="data/stock_analysis/";
    static String outputPath ="data/stock_analysis/output/";

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        StockAnalysis app = new StockAnalysis();
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("StockAnalysis").getOrCreate();
        Dataset<Row> symbolMetadataDF= app.processSymbolMetadata(sparkSession).persist();
        Dataset<Row> stockSymbolDF= app.prepareStockSymbolDF(sparkSession,symbolMetadataDF).persist();
        app.publishSummaryReport(stockSymbolDF,"Summary Report (All Time)","summaryReport");
        String[]sectors =new String[]{"TECHNOLOGY","FINANCE"};
        app.publishTimelyReport(sparkSession,"2021-01-01","2021-05-26",sectors,stockSymbolDF);
        app.publishTimelyPerSector(sparkSession,"2021-01-01","2021-05-26",sectors,stockSymbolDF);

    }
    private Dataset<Row> processSymbolMetadata(SparkSession sparkSession){
        StructType symbolMetadataSchema = StockAnalysisUtils.getSymbolMetadataSchema();
        Dataset<Row> symbolMetadataDF = sparkSession.read().format("csv")
                .option("header", "true")
                .schema(symbolMetadataSchema)
                .load(inputPath+"symbol_metadata.csv");
        symbolMetadataDF=symbolMetadataDF.dropDuplicates();
        //symbolMetadataDF.show(2);
        return symbolMetadataDF;
    }

    private Dataset<Row> prepareStockSymbolDF(SparkSession sparkSession,Dataset<Row> symbolMetadataDF) {
        StructType symbolStockSchema = StockAnalysisUtils.getSymbolStockSchema();
        StructType stockSchema = StockAnalysisUtils.getStockSchema();
        Dataset<Row>symbolStockDF=StockAnalysisUtils.createEmptyDF(sparkSession,symbolStockSchema);

        List<Row> symbolList = symbolMetadataDF.collectAsList();
        for(Row row :symbolList){
            String sectorName = row.getAs("Sector");
            String symbol =row.getAs("Symbol");
            String symbolName = row.getAs("Name");
            Dataset<Row>stockDF=sparkSession.read().format("csv")
                    .option("header", "true")
                    .schema(stockSchema)
                    .option("mode", "DROPMALFORMED")
                    .load(inputPath+symbol+".csv");
            stockDF=stockDF.withColumn("Symbol",lit(symbol))
                    .withColumn("Sector",lit(sectorName))
                    .withColumn("Name",lit(symbolName));
            symbolStockDF=symbolStockDF.unionByName(stockDF);
        }
        //symbolStockDF.show(5);
       // symbolStockDF.printSchema();
       return symbolStockDF;
    }

    private void publishSummaryReport(Dataset<Row> stockSymbolDF,String reportName,String outputFolder) {
        stockSymbolDF=stockSymbolDF
                .groupBy("Sector")
                .agg(round(avg("open"),2).cast("String").alias("Avg Open Price"),
                     round(avg("close"),2).alias("Avg Close Price"),
                     max("high").alias("Max High Price"),
                     min("low").alias("Min Low Price"),
                     round(avg("volume"),0).cast("String").alias("Avg Volume"));
       System.out.println("<=========> Printing "+reportName+" <=========>");
       stockSymbolDF.show(false);
       stockSymbolDF.coalesce(1).write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath+outputFolder);
    }

    private void publishTimelyReport(SparkSession sparkSession,String startDate, String endDate, String[]sectors,Dataset<Row> stockSymbolDF){
        Set<String> set = Arrays.stream(sectors).collect(Collectors.toSet());
        Dataset<Row> filteredDateDF =stockSymbolDF
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).geq(startDate))
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).leq(endDate))
                .filter((FilterFunction<Row>)r->set.contains(r.getAs("Sector")));
       // filteredDateDF.show(5);
        publishSummaryReport(filteredDateDF ,"Performance analysis of each sector","perfAnalysisReport");
    }

    private void publishTimelyPerSector(SparkSession sparkSession,String startDate, String endDate, String[]sectors,Dataset<Row> stockSymbolDF){
        Set<String> set = Arrays.stream(sectors).collect(Collectors.toSet());
        Dataset<Row> filteredDateDF =stockSymbolDF
                .filter((FilterFunction<Row>)r->set.contains(r.getAs("Sector")))
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).geq(startDate))
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).leq(endDate));
        //filteredDateDF.show(5);
        publishTimelyPerSector(filteredDateDF ,"Symbol wise aggregate per Sector","symbolAggReport");
    }

    private void publishTimelyPerSector(Dataset<Row> stockSymbolDF,String reportName,String outputFolder) {;
        stockSymbolDF=stockSymbolDF
                .groupBy("Sector","Name")
                .agg(round(avg("open"),2).cast("String").alias("Avg Open Price"),
                        round(avg("close"),2).alias("Avg Close Price"),
                        max("high").alias("Max High Price"),
                        min("low").alias("Min Low Price"),
                        round(avg("volume"),0).cast("String").alias("Avg Volume"))
                .orderBy("Sector");
        System.out.println("<=========> Printing "+reportName+" <=========>");
        stockSymbolDF.show(false);
        stockSymbolDF.coalesce(1).write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath+outputFolder);
    }
}

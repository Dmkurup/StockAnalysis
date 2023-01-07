package org.squareshift;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class StockAnalysis {
    static Logger log = LogManager.getRootLogger();

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        StockAnalysis app = new StockAnalysis();
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("StockAnalysis").getOrCreate();
        Dataset<Row> symbolMetadataDF= app.processSymbolMetadata(sparkSession).persist();
        Dataset<Row> stockSymbolDF= app.prepareStockSymbolDF(sparkSession,symbolMetadataDF).persist();
        app.publishSummaryReport(stockSymbolDF,"Summary Report (All Time)");
        String[]sectors =new String[]{"TECHNOLOGY","FINANCE"};
        app.publishTimelyReport(sparkSession,"2021-01-01","2021-05-26",sectors,stockSymbolDF);
        app.publishTimelyPerSector(sparkSession,"2021-01-01","2021-05-26",sectors,stockSymbolDF);

    }
    private Dataset<Row> processSymbolMetadata(SparkSession sparkSession){
        StructType symbolMetadataSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Symbol",DataTypes.StringType, false),
                DataTypes.createStructField("Name",DataTypes.StringType, false),
                DataTypes.createStructField("Country",DataTypes.StringType, false),
                DataTypes.createStructField("Sector",DataTypes.StringType, false),
                DataTypes.createStructField("Industry",DataTypes.StringType, false),
                DataTypes.createStructField("Address",DataTypes.StringType, false)
        });
        Dataset<Row> symbolMetadataDF = sparkSession.read().format("csv")
                .option("header", "true")
                .schema(symbolMetadataSchema)
                .load("data/stock_analysis/symbol_metadata.csv");
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
                    .load("data/stock_analysis/"+symbol+".csv");
            stockDF=stockDF.withColumn("Symbol",lit(symbol))
                    .withColumn("Sector",lit(sectorName))
                    .withColumn("Name",lit(symbolName));
            symbolStockDF=symbolStockDF.unionByName(stockDF);
        }
        //symbolStockDF.show(5);
       // symbolStockDF.printSchema();
       return symbolStockDF;
    }

    private void publishSummaryReport(Dataset<Row> stockSymbolDF,String reportName) {
        stockSymbolDF=stockSymbolDF
                .groupBy("Sector")
                .agg(round(avg("open"),2).cast("String").alias("Avg Open Price"),
                     round(avg("close"),2).alias("Avg Close Price"),
                     max("high").alias("Max High Price"),
                     min("low").alias("Min Low Price"),
                     round(avg("volume"),0).cast("String").alias("Avg Volume"));
       System.out.println("<=========> Printing "+reportName+" <=========>");
       stockSymbolDF.show(false);
    }

    private void publishTimelyReport(SparkSession sparkSession,String startDate, String endDate, String[]sectors,Dataset<Row> stockSymbolDF){
        Set<String> set = Arrays.stream(sectors).collect(Collectors.toSet());
        Dataset<Row> filteredDateDF =stockSymbolDF
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).geq(startDate))
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).leq(endDate))
                .filter((FilterFunction<Row>)r->set.contains(r.getAs("Sector")));
       // filteredDateDF.show(5);
        publishSummaryReport(filteredDateDF ,"Performance analysis of each sector");
    }

    private void publishTimelyPerSector(SparkSession sparkSession,String startDate, String endDate, String[]sectors,Dataset<Row> stockSymbolDF){
        Set<String> set = Arrays.stream(sectors).collect(Collectors.toSet());
        Dataset<Row> filteredDateDF =stockSymbolDF
                .filter((FilterFunction<Row>)r->set.contains(r.getAs("Sector")))
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).geq(startDate))
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).leq(endDate));
        //filteredDateDF.show(5);
        publishTimelyPerSector(filteredDateDF ,"Symbol wise aggregate per Sector");
    }

    private void publishTimelyPerSector(Dataset<Row> stockSymbolDF,String reportName) {;
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
    }
}

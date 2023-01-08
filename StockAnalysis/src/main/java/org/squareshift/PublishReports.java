package org.squareshift;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class PublishReports {
    final String outputPath ="data/stock_analysis/output/";

    public void publishAllTimeSummaryReport(Dataset<Row> stockSymbolDF, String reportName, String outputFolder) {
        stockSymbolDF=stockSymbolDF
                .groupBy("Sector")
                .agg(round(avg("open"),2).alias("Avg Open Price"),
                        round(avg("close"),2).alias("Avg Close Price"),
                        round(max("high"),2).alias("Max High Price"),
                        round(min("low"),2).alias("Min Low Price"),
                        round(avg("volume"),2).alias("Avg Volume"));
        System.out.println("<=========> Printing "+reportName+" <=========>");
        stockSymbolDF.show(false);
        //stockSymbolDF.printSchema();
        stockSymbolDF.coalesce(1).write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath+outputFolder);
    }

    public void publishTimelyPerfAnalysisReport(String startDate, String endDate, String[]sectors,Dataset<Row> stockSymbolDF){
        Set<String> set = Arrays.stream(sectors).collect(Collectors.toSet());
        Dataset<Row> filteredDateDF =stockSymbolDF
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).geq(startDate))
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).leq(endDate))
                .filter((FilterFunction<Row>) r->set.contains(r.getAs("Sector")));
        // filteredDateDF.show(5);
        publishAllTimeSummaryReport(filteredDateDF ,"Performance analysis of each sector","perfAnalysisReport");
    }

    public void publishSymbolAggReport(String startDate, String endDate, String[]sectors,Dataset<Row> stockSymbolDF){
        Set<String> set = Arrays.stream(sectors).collect(Collectors.toSet());
        Dataset<Row> filteredDateDF =stockSymbolDF
                .filter((FilterFunction<Row>)r->set.contains(r.getAs("Sector")))
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).geq(startDate))
                .filter(stockSymbolDF.col("timestamp").cast(DataTypes.TimestampType).leq(endDate));
        //filteredDateDF.show(5);
        publishAggBySectorSymbol(filteredDateDF ,"Symbol wise aggregate per Sector","symbolAggReport");
    }

    private void publishAggBySectorSymbol(Dataset<Row> stockSymbolDF,String reportName,String outputFolder) {;
        stockSymbolDF=stockSymbolDF
                .groupBy("Sector","Name")
                .agg(round(avg("open"),2).alias("Avg Open Price"),
                        round(avg("close"),2).alias("Avg Close Price"),
                        round(max("high"),2).alias("Max High Price"),
                        round(min("low"),2).alias("Min Low Price"),
                        round(avg("volume").cast(DataTypes.FloatType),2).alias("Avg Volume"))
                .orderBy("Sector");
        System.out.println("<=========> Printing "+reportName+" <=========>");
        stockSymbolDF.show(false);
        stockSymbolDF.coalesce(1).write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath+outputFolder);
    }
}

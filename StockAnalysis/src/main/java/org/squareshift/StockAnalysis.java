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

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class StockAnalysis {
    static Logger log = LogManager.getRootLogger();

    Dataset<Row> symbolMetadataDF;
    Dataset<Row> stockSymbolDF;

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        StockAnalysis app = new StockAnalysis();
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("StockAnalysis").getOrCreate();
        app.prepareDatasets(sparkSession);
        String startDate ="2021-01-01";
        String endDate ="2021-05-26";
        String[]sectors =new String[]{"TECHNOLOGY","FINANCE"};
        app.publishReports(startDate,endDate,sectors);
    }


    private void prepareDatasets(SparkSession sparkSession){
        PrepareDatasets stockAnalysisDataPrep = new PrepareDatasets(sparkSession);
        this.symbolMetadataDF= stockAnalysisDataPrep.prepareSymbolMetadataDF().persist();
        this.stockSymbolDF= stockAnalysisDataPrep.prepareStockSymbolDF(symbolMetadataDF).persist();
    }

    private void publishReports(String startDate, String endDate, String[]sectors){
        PublishReports publishReports = new PublishReports();
        publishReports.publishAllTimeSummaryReport(this.stockSymbolDF,"Summary Report (All Time)","summaryReport");
        publishReports.publishTimelyPerfAnalysisReport(startDate,endDate,sectors,this.stockSymbolDF);
        publishReports.publishSymbolAggReport(startDate,endDate,sectors,this.stockSymbolDF);
    }


}

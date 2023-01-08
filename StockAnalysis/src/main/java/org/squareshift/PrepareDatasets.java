package org.squareshift;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static org.apache.spark.sql.functions.lit;

public class PrepareDatasets {

    SparkSession sparkSession ;
    final String inputPath ="data/stock_analysis/";
    public PrepareDatasets(SparkSession sparkSession){
        this.sparkSession =sparkSession;
    }

    public  Dataset<Row> prepareSymbolMetadataDF(){
        StructType symbolMetadataSchema = StockAnalysisUtils.getSymbolMetadataSchema();
        Dataset<Row> symbolMetadataDF = this.sparkSession.read().format("csv")
                .option("header", "true")
                .schema(symbolMetadataSchema)
                .load(inputPath+"symbol_metadata.csv");
        symbolMetadataDF=symbolMetadataDF.dropDuplicates();
        //symbolMetadataDF.show(2);
        return symbolMetadataDF;
    }

    public  Dataset<Row> prepareStockSymbolDF( Dataset<Row> symbolMetadataDF) {
        StructType symbolStockSchema = StockAnalysisUtils.getSymbolStockSchema();
        StructType stockSchema = StockAnalysisUtils.getStockSchema();
        Dataset<Row>symbolStockDF=StockAnalysisUtils.createEmptyDF(this.sparkSession,symbolStockSchema);

        List<Row> symbolList = symbolMetadataDF.collectAsList();
        for(Row row :symbolList){
            String sectorName = row.getAs("Sector");
            String symbol =row.getAs("Symbol");
            String symbolName = row.getAs("Name");
            Dataset<Row>stockDF=this.sparkSession.read().format("csv")
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
}

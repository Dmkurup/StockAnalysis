package org.squareshift;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class StockAnalysisUtils {

    public static StructType getSymbolStockSchema() {
        StructType symbolStockSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Symbol", DataTypes.StringType, false),
                DataTypes.createStructField("Sector", DataTypes.StringType, false),
                DataTypes.createStructField("Name", DataTypes.StringType, false),
                DataTypes.createStructField("timestamp", DataTypes.DateType, false),
                DataTypes.createStructField("open", DataTypes.StringType, false),
                DataTypes.createStructField("high", DataTypes.StringType, false),
                DataTypes.createStructField("low", DataTypes.StringType, false),
                DataTypes.createStructField("close", DataTypes.StringType, false),
                DataTypes.createStructField("volume", DataTypes.StringType, false)
        });
        return symbolStockSchema;
    }

    public static StructType getStockSchema() {
        StructType stockSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("timestamp", DataTypes.DateType, false),
                DataTypes.createStructField("open", DataTypes.StringType, false),
                DataTypes.createStructField("high", DataTypes.StringType, false),
                DataTypes.createStructField("low", DataTypes.StringType, false),
                DataTypes.createStructField("close", DataTypes.StringType, false),
                DataTypes.createStructField("volume", DataTypes.StringType, false)
        });
        return stockSchema;
    }
    public static Dataset<Row> createEmptyDF(SparkSession sparkSession,StructType schema){
        List<Row> rows = new ArrayList<Row>();
        return sparkSession.createDataFrame(rows,schema);
    }
}

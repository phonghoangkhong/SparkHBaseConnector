package org.example;

import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
public class Spark  implements Serializable {
    public static void main(String args[]){
            Spark spark=new Spark();
            spark.run();
    }

    void run() {
//        SparkSession ss = SparkSession.builder()
////                .appName("GetTime")
////                .config("spark.yarn.access.hadoopFileSystems", hdfs + "," + hdfs1 + "," + hdfs2)
////                .config("spark.sql.caseSensitive", true)
////                .config("spark.sql.files.ignoreCorruptFiles", true)
////                .getOrCreate();
//        Dataset<Row> a = ss.read().parquet("crossbrowser/CheckMb/*");
//        long total = a.count();
//        System.out.println(total);
//        a = a.groupBy("phone").count().filter("count >= 2");
//        long fact  = a.count();
//        System.out.println(fact);
//        System.out.println((double)fact/(double)total);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkSession ss = SparkSession.builder()
                .appName("Test")
                .config("spark.yarn.access.hadoopFileSystems", "hdfs://10.5.38.1:8020/")
                .getOrCreate();
        Dataset<Row> result = ss.read().parquet("hdfs://10.5.38.1:8020/user/phongkh/part-00000-6a4d184f-4278-42af-b969-fce352e17fb5-c000.snappy.parquet");


        String catalog = "{\n" +
                "\t\"table\":{\"namespace\":\"default\", \"name\":\"phongkh\", \"tableCoder\":\"PrimitiveType\"},\n" +
                "    \"rowkey\":\"idads\",\n" +
                "    \"columns\":{\n" +
                "\t    \"idads\":{\"cf\":\"rowkey\", \"col\":\"idads\", \"type\":\"long\"},\n" +
                "\t    \"phone\":{\"cf\":\"f1\", \"col\":\"phone\", \"type\":\"string\"}\n" +
                "    }\n" +
                "}";

        result
                .write()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .option(HBaseTableCatalog.newTable(), "10")
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save();


//        thongkeMapGuidUUID(ss);
//        thongkeMapAdmicroVMG(ss);
//        checkPhoneFinger(ss,"2020-03-26");
    }

    public static String UUIDtoString(byte[] byteUUID) {
        String uuid = "";
        for (int i = 0; i < 16; i++) {
            if (i == 15) {
                uuid += byteUUID[i];
            } else {
                uuid += byteUUID[i] + "_";

            }
        }
        return uuid;
    }




}
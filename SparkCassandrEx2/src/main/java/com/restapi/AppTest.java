package com.restapi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

public class AppTest {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setAppName("Cassandra-Spark connector.");
		conf.setMaster("local");
		conf.set("spark.cassandra.connection.host", "localhost");

		// RDD
		JavaSparkContext jsc = new JavaSparkContext(conf);
		CassandraTableScanJavaRDD<CassandraRow> cas = CassandraJavaUtil.javaFunctions(jsc).cassandraTable("test",
				"movies");
		System.out.println(cas.collect().toString());
		jsc.stop();

	}
}

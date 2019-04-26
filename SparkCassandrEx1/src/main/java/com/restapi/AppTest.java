package com.restapi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class AppTest {

	private transient SparkConf conf;

	private AppTest(SparkConf conf) {
		this.conf = conf;
	}

	private void run() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		createSchema(sc);
		sc.stop();
	}

	private void createSchema(JavaSparkContext sc) {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());

		Session session = connector.openSession();

		long start = System.currentTimeMillis();
		ResultSet results = session.execute("SELECT * FROM test.itemprice " + "limit 1000");
		long end = System.currentTimeMillis();
		for (Row row : results)
			System.out.println(row.getInt("partnumber") + ":" + row.getDouble("offerprice"));

		System.out.println("time taken : " + (end - start));

	}

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setAppName("Spark");
		conf.setMaster("local");
		conf.set("spark.cassandra.connection.host", "127.0.0.1");

		AppTest app = new AppTest(conf);
		app.run();
	}
}

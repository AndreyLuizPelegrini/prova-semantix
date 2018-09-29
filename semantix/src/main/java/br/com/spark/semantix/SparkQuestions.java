package br.com.spark.semantix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkQuestions {

	private static final List<String> FILES_TO_PROCESS = Arrays.asList("access_log_Aug95.txt", "access_log_Jul95.txt");
	private SparkConf sparkConf;
	private JavaSparkContext sparkContext;

	JavaRDD<String> resultRDD = null;

	public SparkQuestions() {
		sparkConf = new SparkConf();
	}

	/**
	 * Exercício 1 - Número​ ​ de​ ​ hosts​ ​ únicos.
	 */
	public void numeroHostsUnicos() {

		sparkConf.setAppName("NUMERO DE HOSTS UNICOS");
		sparkContext = new JavaSparkContext(sparkConf);
		resultRDD = new JavaRDD<>(null, null);

		for (String arquivo : FILES_TO_PROCESS) {
			JavaRDD<String> log = sparkContext.textFile("hdfs://" + arquivo);
			JavaRDD<String> hosts = log.map(s -> s.split(" ")[0]);
			resultRDD.union(hosts);
		}

		resultRDD.distinct().foreach(h -> System.out.println(h));

	}

	/**
	 * Exercício 2 - O​ ​ total​ ​ de​ ​ erros​ ​ 404.
	 */
	public void total404() {

		sparkConf.setAppName("NUMERO DE 404");
		sparkContext = new JavaSparkContext(sparkConf);
		resultRDD = new JavaRDD<>(null, null);

		for (String arquivo : FILES_TO_PROCESS) {
			JavaRDD<String> log = sparkContext.textFile("hdfs://" + arquivo);
			JavaRDD<String> requests404 = log.map(s -> s.split(" ")[7]).filter(f -> f.equals("404"));
			resultRDD.union(requests404);
		}

		System.out.println(resultRDD.count());

	}

	/**
	 * Exercício 3 - Os​ ​ 5 ​ ​ URLs​ ​ que​ ​ mais​ ​ causaram​ ​ erro​ ​ 404.
	 */

	public void urlsQueMaisCausaram404() {

		sparkConf.setAppName("URL'S QUE MAIS CAUSARAM 404");
		sparkContext = new JavaSparkContext(sparkConf);
		
		Map<String, Long> countByKey = null;

		for (String arquivo : FILES_TO_PROCESS) {
			JavaRDD<String> log = sparkContext.textFile("hdfs://" + arquivo);

			JavaPairRDD<String, Integer> urls = log.flatMap(s -> Arrays.asList(s.split("GET")[1]).iterator())
					.filter(s -> s.split(" ")[3].equals("404"))
					.mapToPair(word -> new Tuple2<>(word.split(" ")[1], 1))
				    .reduceByKey((a, b) -> a + b);
			
			countByKey = urls.countByKey();
			
		}
		
		List<Long> values = new ArrayList<>(countByKey.values());
		Collections.sort(values);
		
		laco_fora: for (int i = 0; i<= 4; i++) {
			
			for (Entry<String, Long> entry : countByKey.entrySet()) {
				if (entry.getValue().equals(values.get(i))) {
					System.out.println(entry.getKey());
					continue laco_fora;
				}
			}
		}		
	}
	
	/**
	 * Exercício 4 - Quantidade​ ​ de​ ​ erros​ ​ 404​ ​ por​ ​ dia.
	 */
	
	public void quantidade404PorDia() {
		sparkConf.setAppName("QUANTIDADE 404 POR DIA");
		sparkContext = new JavaSparkContext(sparkConf);
		
		JavaPairRDD<String, Integer> qtdFinal = new JavaPairRDD<>(null, null, null);

		for (String arquivo : FILES_TO_PROCESS) {
			JavaRDD<String> log = sparkContext.textFile("hdfs://" + arquivo);
			
			JavaPairRDD<String, Integer> qtde404PorDia = log.flatMap(s -> Arrays.asList(s.split("[")).iterator())
					.filter(s -> s.split(" ")[4].equals("404"))
					.mapToPair(word -> new Tuple2<>(word.substring(0, 10), 1))
					.reduceByKey((a, b) -> a + b);
			
			qtdFinal.union(qtde404PorDia);
			
		}
		
		qtdFinal.foreach(data -> System.out.println("Day: " + data._1() + ", Qtd: " + data._2()));
		
		
		
	}

	/**
	 * Exercício 5 - O​ ​ total​ ​ de​ ​ bytes​ ​ retornados.
	 */
	public void totalBytesRetornados() {

		sparkConf.setAppName("TOTAL DE BYTES RETORNADOS");
		sparkContext = new JavaSparkContext(sparkConf);
		resultRDD = new JavaRDD<>(null, null);

		for (String arquivo : FILES_TO_PROCESS) {
			JavaRDD<String> log = sparkContext.textFile("hdfs://" + arquivo);
			JavaRDD<String> bytes = log.map(s -> s.split(" ")[8]);
			resultRDD.union(bytes);
		}

		long count = resultRDD.collect().stream().map(v -> v).count();
		System.out.println(count);

	}
	
}
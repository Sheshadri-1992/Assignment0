package in.ds256.Assignment0;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * DS-256 Assignment 0 Code for generating frequency distribution per hashtag
 */
public class FreqTag implements Serializable {

	public class UserHashtagCount implements Serializable {

		private static final long serialVersionUID = 1L;
		public String userId = "";
		public long totalHashTags;
		public long totalTweets;

		public UserHashtagCount(String argUserId, long argHashtagCount, long argTotalTweet) {
			userId = argUserId;
			totalHashTags = argHashtagCount;
			totalTweets = argTotalTweet;
		}

		public void addHashTags(long hashTags) {
			totalHashTags = totalHashTags + hashTags;
		}

		public void addTweets(long tweets) {
			totalTweets = totalTweets + tweets;
		}

		public String getUser() {
			return userId;
		}

		public long getTotalHashTags() {
			return totalHashTags;
		}

		public long getTotalTweets() {
			return totalTweets;
		}
	}

	/** This is for mapPartitionsToPair **/
	public PairFlatMapFunction<Iterator<String>, String, UserHashtagCount> myTag = new PairFlatMapFunction<Iterator<String>, String, UserHashtagCount>() {

		@Override
		public Iterator<Tuple2<String, UserHashtagCount>> call(Iterator<String> t) throws Exception {

			ArrayList<Tuple2<String, UserHashtagCount>> iter = new ArrayList<Tuple2<String, UserHashtagCount>>();
			// TODO Auto-generated method stub

			Parser myParse = new Parser();

			while (t.hasNext()) {
				String jsonObject = t.next();

				if (jsonObject == null || jsonObject.isEmpty())
					continue;

				myParse.setInputJson(jsonObject);

				String userName = myParse.getUser();				
				if (userName == null)
					continue;

				try {
					UserHashtagCount userHasCount = new UserHashtagCount(userName, myParse.getHashTags(), 1);

					Tuple2<String, UserHashtagCount> myTuple = new Tuple2<String, FreqTag.UserHashtagCount>(userName,
							userHasCount);
					iter.add(myTuple);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}

			return iter.iterator();
		}
	};

	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf().setAppName("FreqTag");
		JavaSparkContext sc = new JavaSparkContext(sparkconf);

		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS

		long startTime = Time.now();

		
		/** Important stuff starts here **/
		FreqTag mySparkObj = new FreqTag();

		JavaRDD<String> inputTweets = sc.textFile(inputFile);

		inputTweets = inputTweets.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String jsonString) throws Exception {

				if (jsonString.contains("\"delete\""))
					return false;

				return true;

			}
		});

		long end = Time.now();

		/** json to pairRdd **/
		JavaPairRDD<String, UserHashtagCount> userHashCountRDD = inputTweets.mapPartitionsToPair(mySparkObj.myTag);

		System.out.println("The number of partions of userHashCountRDD are " + userHashCountRDD.getNumPartitions());

		end = Time.now();
		System.out.println("The time taken in seconds is  after map partions to pair " + (end - startTime) / 1000);

		/** Aggregating the things **/
		JavaPairRDD<String, UserHashtagCount> groupedHashCount = userHashCountRDD.reduceByKey(
				new Function2<FreqTag.UserHashtagCount, FreqTag.UserHashtagCount, FreqTag.UserHashtagCount>() {

					@Override
					public UserHashtagCount call(UserHashtagCount v1, UserHashtagCount v2) throws Exception {
						v1.addHashTags(v2.getTotalHashTags());
						v1.addTweets(v2.getTotalTweets());

						return v1;
					}
				});

		groupedHashCount = groupedHashCount.cache();

		/** Printing sample, to check the results **/
//		for (UserHashtagCount item : groupedHashCount.values().top(5)) {
//
//			Double hashPerTweet = (double) ((double) item.getTotalHashTags() / (double) item.getTotalTweets());
//			System.out.println("The user is " + item.getUser() + " total Tweets are " + item.getTotalTweets()
//					+ " hashtags are " + item.getTotalHashTags() + " ratio is " + hashPerTweet);
//		}

		JavaPairRDD<Integer, Long> countRDD = groupedHashCount
				.mapToPair(new PairFunction<Tuple2<String, UserHashtagCount>, Integer, Long>() {

					@Override
					public Tuple2<Integer, Long> call(Tuple2<String, UserHashtagCount> tuple) throws Exception {

						UserHashtagCount userHashRatio = tuple._2;
						Double hashPerTweet = 0.0;

						if (userHashRatio.getTotalTweets() == 0) {
							Tuple2<Integer, Long> myTuple = new Tuple2<Integer, Long>(0, (long) 0);
							return myTuple;
						}

						hashPerTweet = (double) ((double) userHashRatio.getTotalHashTags()
								/ (double) userHashRatio.getTotalTweets());

						System.out.println(" The hashpertweet is " + hashPerTweet);

						int bucket = hashPerTweet.intValue(); // eg 0.14 will be bucket 0, 1.2 will be bucket 1, 5 will be bucket 5
						Tuple2<Integer, Long> myTuple = new Tuple2<Integer, Long>(bucket, (long) 1);

						return myTuple;
					}

				});

		/** Aggregating each bucket's value**/
		countRDD = countRDD.reduceByKey(new Function2<Long, Long, Long>() { // First Integer is bucket, Second bucket is for count

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		});

		long num = countRDD.count();

		ArrayList<Integer> tweetBuckets = new ArrayList<Integer>();

		for (Integer item : countRDD.keys().collect()) {
			System.out.println("They key is "+item);
			tweetBuckets.add(item);
		}

		ArrayList<Long> userCounts = new ArrayList<Long>();

		for (Long item : countRDD.values().collect()) {
			System.out.println("The value is "+item);
			userCounts.add(item);
		}

		JavaRDD<Integer> bucketRDD = sc.parallelize(tweetBuckets);
		bucketRDD.coalesce(1).saveAsTextFile(outputFile+"/keys");
		
		JavaRDD<Long> userCountRDD = sc.parallelize(userCounts);
		userCountRDD.coalesce(1).saveAsObjectFile(outputFile+"/values");

		end = Time.now();

		System.out.println("The time taken in seconds is  " + (end - startTime) / 1000);

		sc.stop();
		sc.close();
	}

}
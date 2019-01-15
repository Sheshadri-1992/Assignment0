package in.ds256.Assignment0;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MyFirstSpark implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public class UserHashtagCount implements Serializable {
		/**
		 * 
		 */
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

	public FlatMapFunction<Tuple2<String, String>, HashMap<String,UserHashtagCount>> pairHashTag = new FlatMapFunction<Tuple2<String, String>, HashMap<String,UserHashtagCount>>() {

		private static final long serialVersionUID = 1L;
		public HashMap<String, UserHashtagCount> globalUserCount = new HashMap<String, MyFirstSpark.UserHashtagCount>();

		@Override
		public Iterator<HashMap<String,UserHashtagCount>> call(Tuple2<String, String> jsonFiles) throws Exception {

			System.out.println("I am in the pairhastag call method");
			ArrayList<HashMap<String,UserHashtagCount>> myIter = new ArrayList<HashMap<String,UserHashtagCount>>();
			System.out.println("Here is the name of the file " + jsonFiles._1());

			List<String> lines = Arrays.asList(jsonFiles._2().split("\\r?\\n"));

			for (String jsonObject : lines) {

				Parser myParse = new Parser(jsonObject);
				if (myParse.checkIfDelete() == true) {
					continue;
				}

				if (globalUserCount.containsKey(myParse.getUser()) == false) {
//					System.out.println("THe dictionary does not contain user count");
					UserHashtagCount userHashTag = new UserHashtagCount(myParse.getUser(), myParse.getHashTags().size(),
							1);
					globalUserCount.put(myParse.getUser(), userHashTag);

				} else {
					globalUserCount.get(myParse.getUser()).addHashTags(myParse.getHashTags().size());
					globalUserCount.get(myParse.getUser()).addTweets(1);
				}

			}

			System.out.println("the total number of values are " + globalUserCount.size());

			myIter.add(globalUserCount);

			return myIter.iterator();
		}

	};
	
	/**/
	public PairFunction<String, String, UserHashtagCount> hasTagFunction = new PairFunction<String,String, UserHashtagCount>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, UserHashtagCount> call(String jsonString) throws Exception {
			
			Parser myParse = new Parser(jsonString);
			String userName = myParse.getUser();
			UserHashtagCount userHasCount = new UserHashtagCount(userName, myParse.getHashTags().size(), 1);
			
			return new Tuple2<String, MyFirstSpark.UserHashtagCount>(userName, userHasCount);
		}
		
		
	};
	
	public PairFlatMapFunction<Iterator<String>, String, UserHashtagCount> myTag = new PairFlatMapFunction<Iterator<String>, String, UserHashtagCount>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<Tuple2<String, UserHashtagCount>> call(Iterator<String> t) throws Exception {
			
//			System.out.println("Here it is ");
			// TODO Auto-generated method stub
			ArrayList<Tuple2<String, UserHashtagCount>> iter = new ArrayList<Tuple2<String,UserHashtagCount>>();
			
			while(t.hasNext()) {
				String jsonObject = t.next();
				Parser myParse = new Parser(jsonObject);
				
				if(myParse.checkIfDelete()) //skip the delete tweets
					continue;
				
				String userName = myParse.getUser();
				UserHashtagCount userHasCount = new UserHashtagCount(userName, myParse.getHashTags().size(), 1);
				
				Tuple2 myTuple =  new Tuple2<String, MyFirstSpark.UserHashtagCount>(userName, userHasCount);
				iter.add(myTuple);
				
			}
			
			return iter.iterator();
		}
	};  

	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf().setMaster("local").setAppName("MyFirstSpark");
		JavaSparkContext sc = new JavaSparkContext(sparkconf);

		long startTime = Time.now();
		// this is creation of a RDD
		JavaRDD<String> linesRDD = sc.parallelize(
				Arrays.asList("Krishna", "Rama", "Krishna was from dwapara yuga", "Rama was from treta yuga"));

		// Lets look at a transformation, lines is a RDD, we applied transformation to
		// it to get krishnaRDD
		JavaRDD<String> krishnaRDD = linesRDD.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String x) throws Exception {
				// TODO Auto-generated method stub
				return x.contains("Krishna");

			}
		});

		// similarly here we applied transformation to get ramaRDD
		JavaRDD<String> ramaRDD = linesRDD.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String x) throws Exception {
				// TODO Auto-generated method stub
				return x.contains("Rama");

			}
		});

		// UnionRDD , operates on 2 RDDs, it is also a type of transformation since it 
		// returns a RDD
		JavaRDD<String> unionRDD = krishnaRDD.union(ramaRDD);

		// count() is an action
		System.out.println("The unionRDD has " + unionRDD.count() + " count"); /// expectation is 4
		System.out.println("Here are the items in the unionRDD");

		for (String item : unionRDD.take(4)) { // take is an action
			System.out.println("Here is the element " + item);
		}

		// RDD of Integers
		JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> squareRDD = intRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer x) throws Exception {
				// TODO Auto-generated method stub
				return x * x;
			}

		});

		// collect() returns all the items of the RDD< use this if the result is small.
		System.out.println("The result is " + squareRDD.collect().toString());

		// flatMap(), the flatMap() returns multiple number of iterators of the element
//		JavaRDD<String> jsonRDD = sc.textFile(Constants.TWEET_FILE_PATH);

		
		/** Important stuff starts here **/
		
		MyFirstSpark mySparkObj = new MyFirstSpark();

		JavaRDD<String> jsonPairRDD = sc.textFile(Constants.TWEET_DIR_PATH,2); //4 is the number of partitions
//		jsonPairRDD = jsonPairRDD.sample(false, 0.01);
		
		//here I removed the deleted tweets
//		jsonPairRDD = jsonPairRDD.filter( new Function<String, Boolean>() {
//			
//			@Override
//			public Boolean call(String jsonString) throws Exception {
//							
//				if(jsonString.contains("\"delete\""))
//					return false;
//				
//				return true;
//			}
//		} );
		
		jsonPairRDD.cache();
		
		long end = Time.now();
		
		System.out.println("Filter operation The time taken in seconds is  "+(end-startTime)/1000);
		
		/**Most crucial function **/
//		JavaPairRDD<String, UserHashtagCount> userHashCountRDD =  jsonPairRDD.mapToPair(mySparkObj.hasTagFunction);
		
		JavaPairRDD<String, UserHashtagCount> userHashCountRDD  = jsonPairRDD.mapPartitionsToPair(mySparkObj.myTag);
		System.out.println("The number of partions of userHashCountRDD are "+userHashCountRDD.getNumPartitions());
		
		end = Time.now();
		System.out.println("The time taken in seconds is  after map partions to pair "+(end-startTime)/1000);
		
		/**Aggregating the things **/
		JavaPairRDD<String, UserHashtagCount> groupedHashCount = userHashCountRDD.reduceByKey( new Function2<MyFirstSpark.UserHashtagCount, MyFirstSpark.UserHashtagCount, MyFirstSpark.UserHashtagCount>() {
			
			@Override
			public UserHashtagCount call(UserHashtagCount v1, UserHashtagCount v2) throws Exception {
//				System.out.println("3");
				v1.addHashTags(v2.getTotalHashTags());
				v1.addTweets(v2.getTotalTweets());
				 
				return v1;
			}
		});
		
		System.out.println("The number of partions are "+groupedHashCount.getNumPartitions());
		
		for(UserHashtagCount item : groupedHashCount.values().take(4)) {
			
			Double hashPerTweet = (double) ((double) item.getTotalHashTags()/ (double) item.getTotalTweets());
			System.out.println("The user is "+item.getUser()+" total Tweets are "+item.getTotalTweets()+" hashtags are "+item.getTotalHashTags()+" ratio is "+hashPerTweet);			
		}
		
		groupedHashCount.saveAsTextFile(Constants.TWEET_OUTPUT_DIR);
		
		end = Time.now();
		
		System.out.println("The time taken in seconds is  "+(end-startTime)/1000);
		
		/**Printing to check the results **/
		/*for(UserHashtagCount item : groupedHashCount.values().collect()) {
			Double hashPerTweet = (double) ((double) item.getTotalHashTags()/ (double) item.getTotalTweets());
			System.out.println("The user is "+item.getUser()+" total Tweets are "+item.getTotalTweets()+" hashtags are "+item.getTotalHashTags()+" ratio is "+hashPerTweet);
			
		}*/
						
		
//		JavaRDD<HashMap<String, UserHashtagCount>> userHashTagObj = jsonPairRDD.flatMap(mySparkObj.pairHashTag);
//		System.out.println("I am back " + userHashTagObj.take(1).size());
		
		
		

//		JavaPairRDD<String, UserHashtagCount> hashTagObj = userHashTagObj
//				.mapToPair(new PairFunction<MyFirstSpark.UserHashtagCount, String, UserHashtagCount>() {
//
//					/**
//					 * 
//					 */
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, UserHashtagCount> call(UserHashtagCount t) throws Exception {
//
//						return new Tuple2<String, UserHashtagCount>(t.getUser(), t);
//
//					}
//				});
		
//		JavaRDD<Double> tweetsPerUser = userHashTagObj.flatMap( new FlatMapFunction<MyFirstSpark.UserHashtagCount, Double>() {
//
//			@Override
//			public Iterator<Double> call(UserHashtagCount userHashCount) throws Exception {
//				// TODO Auto-generated method stub
//				
//				ArrayList<Double> iter = new ArrayList<Double>();
//				
//				Double hashPerTweet = (double) ((double) userHashCount.getTotalHashTags()/ (double) userHashCount.getTotalTweets());
////
//				System.out.println("totalHashTags: "+userHashCount.getTotalHashTags()+" totalTweets : "+userHashCount.getTotalTweets()+ " : "+" Hashtag ratio "+hashPerTweet);
//				iter.add(hashPerTweet);
//				
//				return iter.iterator();
//			}
//		});
				
//		System.out.println("Number of tweets per user "+tweetsPerUser.count());

//		System.out.println("MAIN Total is " + hashTagObj.count());
//
//		hashTagObj = hashTagObj.distinct();	
//
//		System.out.println("I am here now" + hashTagObj.count());

//		JavaRDD<Double> myJavaRDD = hashTagObj.map(new Function<Tuple2<String, UserHashtagCount>, Double>() {
//
//			@Override
//			public Double call(Tuple2<String, UserHashtagCount> tuple) throws Exception {
//
//				UserHashtagCount userHashCount = tuple._2();
////				
//				Double hashPerTweet = (double) ((double) userHashCount.getTotalHashTags()
//						/ (double) userHashCount.getTotalTweets());
//
//				System.out.println("I am in the final call, hashpertweet for the user is " + hashPerTweet);
//
//				return hashPerTweet;
//			}
//		});
//
//		System.out.println("it's over now " + myJavaRDD.count());

//		JavaPairRDD<String, Double> hashTagsPerTweetPerUser = hashTagObj.mapToPair( new PairFunction<Tuple2<String,UserHashtagCount>, String, Double>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, Double> call(Tuple2<String, UserHashtagCount> t) throws Exception {
//				// TODO Auto-generated method stub
//				System.out.println("In the final method");
//				String user = t._1();
//				UserHashtagCount userHashCount = t._2();
//				
//				Double hashPerTweet =  (double) ((double)userHashCount.getTotalHashTags()/(double)userHashCount.getTotalTweets());
//				return new Tuple2<String, Double>(user, hashPerTweet);
//			}
//		});
//		
//		hashTagObj.count();

//		hashTagObj.saveAsTextFile(Constants.OUTPUT_DIR);
//		
//		for (UserHashtagCount obj : hashTagObj.values().collect()) {
//
//			System.out.println("The user is " + obj.getUser());
//			System.out.println("Tweets are " + obj.getTotalTweets()+" : "+"HashTags are " + obj.getTotalHashTags());
//			
//		}

//		for(Double item: hashTagsPerTweetPerUser.values().collect()) {
//			System.out.println("The   is "+item);
//		}
		System.out.println("This should be over");
		sc.stop();
		sc.close();
	}

}

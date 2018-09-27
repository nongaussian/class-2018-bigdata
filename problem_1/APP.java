import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
 
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
 
import scala.Tuple2;
 
//All pair partitioning
public class APP {
	static int GROUP = 2;
	// static double THRESHOLD = 0.1;
 
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("All pair partitioning");
 
		JavaSparkContext ctx = new JavaSparkContext(conf);
 
		String filePath = args[0];
		double THRESHOLD = Double.parseDouble(args[1]);
		JavaRDD<String> data = ctx.textFile(filePath);
 
		// Map
		// @ return: <K, V>: <Key, Val>
		//   Key: <K, V>: Tuple2 consists of GroupID, HashNum (ascending order) <GroupID, HashNum> or <HashNum, GroupID>
		//   Val: <K, V>: <HashNum, friends>
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> partitioning = data.flatMapToPair(p -> {
			int h = Integer.parseInt(p.split(",")[0].trim()) % GROUP;
			List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, String>>> result = new ArrayList<>();
 
			// Fill in here
 
			return result.iterator();
		});
 
		// Reduce
		// @ return: <K, V>: <pair of IDs, Jaccard similarity>
		JavaPairRDD<String, Double> jaccard = partitioning.groupByKey().flatMapToPair(g -> {
			// @ tupleList: Group by HashNum 
			List<Tuple2<Integer, String>>[] tupleList = null;
			if (g._1()._1() == g._1()._2()) {
				tupleList = new ArrayList[1];
			} else {
				tupleList = new ArrayList[2];
			}
 
			for (int i = 0; i < tupleList.length; i++) {
				tupleList[i] = new ArrayList<>();
			}
 
			int index0 = g._1()._1();
			for (Tuple2<Integer, String> p : g._2()) {
				if (p._1() == index0) {
					tupleList[0].add(p);
				} else {
					tupleList[1].add(p);
				}
			}
 
			List<Tuple2<String, Double>> result = new ArrayList<>();
 
			// Fill in here
			// calculate Jaccard similarity
 
			return result.iterator();
		});
 
		for (Tuple2<String, Double> aa : jaccard.collect()) {
			System.out.println(aa);
		}
 
		System.out.println("Total: " + jaccard.count());
 
		ctx.close();
	}
 
}
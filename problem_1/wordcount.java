import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
public class WordCount {
        public static void main(String[] args) {
                SparkConf conf = new SparkConf();
                conf.setAppName("WordCount");

                // This line should be always included
                JavaSparkContext ctx = new JavaSparkContext(conf);

                // Read file
                String filePath = args[0];
                JavaRDD<String> data = ctx.textFile(filePath);

                // Flat lines from file
                JavaRDD<String> words = data.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

                // Map: key = word, value = 1
                JavaPairRDD<String, Integer> pair = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));

                // Reduce: for each keys, add values
                JavaPairRDD<String, Integer> wordCounts = pair.reduceByKey((a, b) -> a + b);

                // Show result
                for(Tuple2<String, Integer> wordCount : wordCounts.collect()) {
                        System.out.println(wordCount._1 + "\t" + wordCount._2);
                }

                ctx.close();
        }
}

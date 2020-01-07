package de.htw.f4.ai;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class MainSpark {

    private static final String FILE = Thread.currentThread().getContextClassLoader().getResource("The_Adventures_of_Tom_Sawyer.txt").getPath();
    private static final String FILE_2 = Thread.currentThread().getContextClassLoader().getResource("The_Adventures_of_Tom_Sawyer_2.txt").getPath();
    private static final String FILE_4 = Thread.currentThread().getContextClassLoader().getResource("The_Adventures_of_Tom_Sawyer_4.txt").getPath();
    private static final String FILE_ALL = Thread.currentThread().getContextClassLoader().getResource("all_test_files.txt").getPath();

    public static void main(String[] args) throws IOException, URISyntaxException {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            HelpFunctions helpFunctions = new HelpFunctions();

            Tuple3<List<Tuple2<String, Integer>>, Long, Long> dumy = countTop10WordsAndTime(FILE, jsc, helpFunctions);
            // (<Top10Words and Count>, WordsCountInCorpus, TimeToCompute)
            Tuple3<List<Tuple2<String, Integer>>, Long, Long> simple_corpus = countTop10WordsAndTime(FILE, jsc, helpFunctions);
            Tuple3<List<Tuple2<String, Integer>>, Long, Long> double_corpus = countTop10WordsAndTime(FILE_2, jsc, helpFunctions);
            Tuple3<List<Tuple2<String, Integer>>, Long, Long> quadratic_corpus = countTop10WordsAndTime(FILE_4, jsc, helpFunctions);
            Tuple3<List<Tuple2<String, Integer>>, Long, Long> all_texts = countTop10WordsAndTime(FILE_ALL, jsc, helpFunctions);


            jsc.stop();
        }
    }

    private static Tuple3<List<Tuple2<String, Integer>>, Long, Long> countTop10WordsAndTime(String file, JavaSparkContext jsc, HelpFunctions helpFunctions) {
        JavaRDD<String> rddLines = jsc.textFile(file, 4);
        long startTime = System.nanoTime();
        JavaRDD<String> rddWords = helpFunctions.splitAndCleanLines(rddLines);
        JavaRDD<Tuple2<String, Integer>> wordsFreqSortedRDD = helpFunctions.countWords(rddWords);
        List<Tuple2<String, Integer>> wordsFreqSorted = wordsFreqSortedRDD.collect();
        long endTime = System.nanoTime();
        long totalTime = (endTime - startTime) / 1000;
        return new Tuple3<>(wordsFreqSorted.subList(0, 10), rddWords.count(), totalTime);
    }
}

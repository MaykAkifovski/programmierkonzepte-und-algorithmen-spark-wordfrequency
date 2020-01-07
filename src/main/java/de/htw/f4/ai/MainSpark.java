package de.htw.f4.ai;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MainSpark {
    private static final String FILE = "The_Adventures_of_Tom_Sawyer.txt";
    private static final String FILE_2 = "The_Adventures_of_Tom_Sawyer_2.txt";
    private static final String FILE_4 = "The_Adventures_of_Tom_Sawyer_4.txt";
    private static final String FILE_ALL = "all_test_files.txt";

    private static List<String> readFromFile(String fileName) {
        try (Stream<String> lines = Files.lines(Paths.get(Objects.requireNonNull(MainJava.class.getClassLoader().getResource(fileName)).toURI()))) {
            return lines.collect(Collectors.toList());
        } catch (URISyntaxException | IOException e) {
            System.out.println("Error in readFromFile() for " + fileName);
            return Collections.emptyList();
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            HelpFunctions helpFunctions = new HelpFunctions();

            List<String> file = readFromFile(FILE);
            List<String> file_2 = readFromFile(FILE_2);
            List<String> file_4 = readFromFile(FILE_4);
            List<String> file_all = readFromFile(FILE_ALL);

            Tuple3<List<Tuple2<String, Integer>>, Long, Long> dumy = countTop10WordsAndTime(file, jsc, helpFunctions);
            // (<Top10Words and Count>, WordsCountInCorpus, TimeToCompute)
            Tuple3<List<Tuple2<String, Integer>>, Long, Long> simple_corpus = countTop10WordsAndTime(file, jsc, helpFunctions);
            Tuple3<List<Tuple2<String, Integer>>, Long, Long> double_corpus = countTop10WordsAndTime(file_2, jsc, helpFunctions);
            Tuple3<List<Tuple2<String, Integer>>, Long, Long> quadratic_corpus = countTop10WordsAndTime(file_4, jsc, helpFunctions);
            Tuple3<List<Tuple2<String, Integer>>, Long, Long> all_texts = countTop10WordsAndTime(file_all, jsc, helpFunctions);


            jsc.stop();
        }
    }

    private static Tuple3<List<Tuple2<String, Integer>>, Long, Long> countTop10WordsAndTime(List<String> file, JavaSparkContext jsc, HelpFunctions helpFunctions) {
        JavaRDD<String> rddLines = jsc.parallelize(file, 4);
        long startTime = System.nanoTime();
        JavaRDD<String> rddWords = helpFunctions.splitAndCleanLines(rddLines);
        JavaRDD<Tuple2<String, Integer>> wordsFreqSortedRDD = helpFunctions.countWords(rddWords);
        List<Tuple2<String, Integer>> wordsFreqSorted = wordsFreqSortedRDD.collect();
        long endTime = System.nanoTime();
        long totalTime = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS);
        return new Tuple3<>(wordsFreqSorted.subList(0, 10), rddWords.count(), totalTime);
    }
}

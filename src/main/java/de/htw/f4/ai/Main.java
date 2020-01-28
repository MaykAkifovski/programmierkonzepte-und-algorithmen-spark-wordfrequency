package de.htw.f4.ai;

import de.htw.f4.ai.compute.Utils;
import de.htw.f4.ai.compute.WordFrequencySpark;
import de.htw.f4.ai.result.Result;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Main {

    private static final String FILE = "one_book.txt";
    private static final String FILE_2 = "two_books.txt";
    private static final String FILE_4 = "four_books.txt";
    private static final String FILE_ALL = "all_books.txt";

    public static void main(String[] args) {
        List<String> stopwords = Utils.readStopwords("dutch");
        List<String> text = Utils.readAllFilesFromDirectory("dutch");

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {

            Result dummy = WordFrequencySpark.run(text, stopwords, 8, jsc);


        }
    }
}

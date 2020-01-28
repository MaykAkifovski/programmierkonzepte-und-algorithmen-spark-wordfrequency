package de.htw.f4.ai;

import de.htw.f4.ai.compute.Utils;
import de.htw.f4.ai.compute.WordFrequencyJava;
import de.htw.f4.ai.compute.WordFrequencyJavaParallel;
import de.htw.f4.ai.compute.WordFrequencySpark;
import de.htw.f4.ai.result.Result;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Main {

    private static final List<String> languages = Arrays.asList(
            "dutch", "english", "french", "german", "italian", "russian", "spanish", "ukrainian"
    );

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            Result dummy = WordFrequencySpark.run(Collections.emptyList(), Collections.emptyList(), 1, jsc);
            languages.forEach(language -> countWordsForLanguage(language, jsc));
        }

    }

    private static void countWordsForLanguage(String language, JavaSparkContext jsc) {
        List<String> stopwords = Utils.readStopwords(language);
        List<String> text = Utils.readAllFilesFromDirectory(language);

        Result resultSpark = WordFrequencySpark.run(text, stopwords, 8, jsc);
        Result resultJava = WordFrequencyJava.run(text, stopwords);
        Result resultJavaParallel = WordFrequencyJavaParallel.run(text, stopwords);

        System.out.printf("top 10 words for language %s: %s \n", language, resultSpark.top10Words);
        System.out.println("total number of words: " + resultSpark.wordsCountInCorpus);

        System.out.println("compute duration (Spark): " + resultSpark.timeToComputeMs + " ms");
        System.out.println("compute duration (Java): " + resultJava.timeToComputeMs + " ms");
        System.out.println("compute duration (Java - parallel): " + resultJavaParallel.timeToComputeMs + " ms");
        System.out.println("----------------------------------------------");
    }
}

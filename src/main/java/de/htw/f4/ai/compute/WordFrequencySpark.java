package de.htw.f4.ai.compute;

import de.htw.f4.ai.result.Result;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WordFrequencySpark {

    public static Result run(List<String> text, List<String> stopWords, int numberOfPartitions, JavaSparkContext jsc) {
        return countTop10WordsAndTime(text, stopWords, numberOfPartitions, jsc);
    }

    private static Result countTop10WordsAndTime(List<String> text, List<String> stopWords, int numberOfPartitions, JavaSparkContext jsc) {
        JavaRDD<String> rddLines = jsc.parallelize(text, numberOfPartitions);
        long startTime = System.nanoTime();

        JavaRDD<String> rddWords = splitAndCleanLines(rddLines, stopWords);
        JavaRDD<Tuple2<String, Integer>> wordsFreqSortedRDD = countWords(rddWords);
        List<Tuple2<String, Integer>> wordsFreqSorted = wordsFreqSortedRDD.collect();

        long endTime = System.nanoTime();
        long totalTime = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS);

        return new Result(
                wordsFreqSorted.subList(0, 10),
                rddWords.count(),
                totalTime
        );
    }

    public static JavaRDD<String> splitAndCleanLines(JavaRDD<String> rddLines, List<String> stopWords) {
        return rddLines
                .map(line -> line.split("\\W+"))
                .map(Arrays::asList)
                .flatMap(List::iterator)
                .map(String::toLowerCase)
                .filter(word -> !stopWords.contains(word));

    }

    private static JavaRDD<Tuple2<String, Integer>> countWords(JavaRDD<String> rddWords) {
        return rddWords
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .map(t -> t)
                .sortBy(pair -> pair._2, false, 4);
    }
}

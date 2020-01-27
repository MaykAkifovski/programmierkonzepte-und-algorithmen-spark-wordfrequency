package de.htw.f4.ai.compute;

import de.htw.f4.ai.result.Result;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordFrequencySpark {

    private static List<String> stopwords;

    private static List<String> readFromFile(String fileName) {
        try (Stream<String> lines = Files.lines(Paths.get(Objects.requireNonNull(WordFrequencyJava.class.getClassLoader().getResource(fileName)).toURI()))) {
            return lines.collect(Collectors.toList());
        } catch (URISyntaxException | IOException e) {
            System.out.println("Error in readFromFile() for " + fileName);
            return Collections.emptyList();
        }
    }

    private static void readStopWords() {
        try (Stream<String> lines = Files.lines(Paths.get(Thread.currentThread().getContextClassLoader().getResource("stopwords.txt").toURI()))) {
            stopwords = lines.collect(Collectors.toList());
            stopwords.add("");
        } catch (IOException | URISyntaxException e) {
            System.out.println("Error in readStopWords()");
        }
    }

    public static Result run(String fileName, int numberOfPartitions, JavaSparkContext jsc) {
        readStopWords();

        List<String> file = readFromFile(fileName);

        Result top10WordsCorpusSizeAndTime = countTop10WordsAndTime(file, numberOfPartitions, jsc);

        return top10WordsCorpusSizeAndTime;
    }

    private static Result countTop10WordsAndTime(List<String> file, int numberOfPartitions, JavaSparkContext jsc) {
        JavaRDD<String> rddLines = jsc.parallelize(file, numberOfPartitions);
        long startTime = System.nanoTime();

        JavaRDD<String> rddWords = splitAndCleanLines(rddLines);
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

    public static JavaRDD<String> splitAndCleanLines(JavaRDD<String> rddLines) {
        List<String> _stopwords = stopwords;
        return rddLines
                .map(line -> line.split("\\W+"))
                .map(Arrays::asList)
                .flatMap(List::iterator)
                .map(String::toLowerCase)
                .filter(word -> !_stopwords.contains(word));

    }

    private static JavaRDD<Tuple2<String, Integer>> countWords(JavaRDD<String> rddWords) {
        return rddWords
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .map(t -> t)
                .sortBy(pair -> pair._2, false, 4);
    }
}

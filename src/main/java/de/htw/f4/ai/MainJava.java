package de.htw.f4.ai;

import scala.Tuple3;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MainJava {
    private static final String FILE = "The_Adventures_of_Tom_Sawyer.txt";
    private static final String FILE_2 = "The_Adventures_of_Tom_Sawyer_2.txt";
    private static final String FILE_4 = "The_Adventures_of_Tom_Sawyer_4.txt";
    private static final String FILE_ALL = "all_test_files.txt";

    private static List<String> stopwords;

    private static void readStopWords() {
        try (Stream<String> lines = Files.lines(Paths.get(Thread.currentThread().getContextClassLoader().getResource("stopwords.txt").toURI()))) {
            stopwords = lines.collect(Collectors.toList());
            stopwords.add("");
        } catch (IOException | URISyntaxException e) {
            System.out.println("Error in readStopWords()");
        }
    }

    private static List<String> readFromFile(String fileName) {
        try (Stream<String> lines = Files.lines(Paths.get(Objects.requireNonNull(MainJava.class.getClassLoader().getResource(fileName)).toURI()))) {
            return lines.collect(Collectors.toList());
        } catch (URISyntaxException | IOException e) {
            System.out.println("Error in readFromFile() for " + fileName);
            return Collections.emptyList();
        }
    }

    public static void main(String[] args) {
        readStopWords();

        List<String> file = readFromFile(FILE);
        List<String> file_2 = readFromFile(FILE_2);
        List<String> file_4 = readFromFile(FILE_4);
        List<String> file_all = readFromFile(FILE_ALL);

        Tuple3<List<Map.Entry<String, Long>>, Long, Long> top10WordsCorpusSizeAndTime_file = countTop10WordsAndTime(file);
        Tuple3<List<Map.Entry<String, Long>>, Long, Long> top10WordsCorpusSizeAndTime_file_2 = countTop10WordsAndTime(file_2);
        Tuple3<List<Map.Entry<String, Long>>, Long, Long> top10WordsCorpusSizeAndTime_file_4 = countTop10WordsAndTime(file_4);
        Tuple3<List<Map.Entry<String, Long>>, Long, Long> top10WordsCorpusSizeAndTime_file_all = countTop10WordsAndTime(file_all);

        System.out.println();
    }

    private static Tuple3<List<Map.Entry<String, Long>>, Long, Long> countTop10WordsAndTime(List<String> lines) {
        long startTime = System.nanoTime();
        Supplier<Stream<String>> words = () -> splitAndCleanLines(lines);
        List<Map.Entry<String, Long>> wordsFreqSorted = countWords(words);
        long endTime = System.nanoTime();
        long totalTime = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS);
        return new Tuple3<>(wordsFreqSorted.subList(0, 10), words.get().count(), totalTime);
    }

    private static Stream<String> splitAndCleanLines(List<String> lines) {
        return lines
                .stream()
                .map(line -> line.split("\\W+"))
                .flatMap(Stream::of)
                .map(String::toLowerCase)
                .filter(word -> !stopwords.contains(word));

    }

    private static List<Map.Entry<String, Long>> countWords(Supplier<Stream<String>> words) {
        return words.get()
//                .parallel()
                .collect(Collectors.groupingBy(String::toString, Collectors.counting()))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());

    }
}

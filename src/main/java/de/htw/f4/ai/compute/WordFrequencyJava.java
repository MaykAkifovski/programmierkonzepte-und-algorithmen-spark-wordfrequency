package de.htw.f4.ai.compute;

import de.htw.f4.ai.result.Result;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordFrequencyJava {

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
        try (Stream<String> lines = Files.lines(Paths.get(Objects.requireNonNull(WordFrequencyJava.class.getClassLoader().getResource(fileName)).toURI()))) {
            return lines.collect(Collectors.toList());
        } catch (URISyntaxException | IOException e) {
            System.out.println("Error in readFromFile() for " + fileName);
            return Collections.emptyList();
        }
    }

    public static Result run(String fileName) {
        readStopWords();

        List<String> file = readFromFile(fileName);

        Result top10WordsCorpusSizeAndTime = countTop10WordsAndTime(file);

        return top10WordsCorpusSizeAndTime;
    }

    private static Result countTop10WordsAndTime(List<String> lines) {
        long startTime = System.nanoTime();

        Supplier<Stream<String>> words = () -> splitAndCleanLines(lines);
        List<Map.Entry<String, Long>> wordsFreqSorted = countWords(words);

        long endTime = System.nanoTime();
        long totalTime = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS);

        return new Result(
                wordsFreqSorted.subList(0, 10),
                words.get().count(),
                totalTime
        );
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
                .collect(Collectors.groupingBy(String::toString, Collectors.counting()))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());

    }
}

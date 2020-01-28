package de.htw.f4.ai.compute;

import de.htw.f4.ai.result.Result;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordFrequencyJava {

    public static Result run(List<String> text, List<String> stopwords) {
        return countTop10WordsAndTime(text, stopwords);
    }

    private static Result countTop10WordsAndTime(List<String> text, List<String> stopwords) {
        long startTime = System.nanoTime();

        Supplier<Stream<String>> words = () -> splitAndCleanLines(text, stopwords);
        List<Map.Entry<String, Long>> wordsFreqSorted = countWords(words);

        long endTime = System.nanoTime();
        long totalTime = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS);

        return new Result(
                wordsFreqSorted.subList(0, 10),
                words.get().count(),
                totalTime
        );
    }

    private static Stream<String> splitAndCleanLines(List<String> text, List<String> stopwords) {
        return text
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

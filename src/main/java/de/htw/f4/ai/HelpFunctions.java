package de.htw.f4.ai;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HelpFunctions {

    private List<String> stopwords;

    public HelpFunctions() throws URISyntaxException, IOException {
        Path path = Paths.get(Thread.currentThread().getContextClassLoader().getResource("stopwords.txt").toURI());
        try (Stream<String> lines = Files.lines(path)) {
            this.stopwords = lines.collect(Collectors.toList());
            this.stopwords.add("");
        }
    }

    public JavaRDD<String> splitAndCleanLines(JavaRDD<String> rddLines) {
        List<String> _stopwords = this.stopwords;
        return rddLines
                .map(line -> line.split("\\W+"))
                .map(Arrays::asList)
                .flatMap(List::iterator)
                .map(String::toLowerCase)
                .filter(word -> !_stopwords.contains(word));

    }

    public JavaRDD<Tuple2<String, Integer>> countWords(JavaRDD<String> rddWords) {
        return rddWords
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .map(t -> t)
                .sortBy(pair -> pair._2, false, 4);
    }
}

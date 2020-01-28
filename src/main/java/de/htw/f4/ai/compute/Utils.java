package de.htw.f4.ai.compute;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

public class Utils {
    public static List<String> readStopwords(String fileName) {
        List<String> stopwords = emptyList();
        try (Stream<String> lines = Files.lines(Paths.get(Thread.currentThread().getContextClassLoader().getResource(fileName + "_stopwords.txt").toURI()))) {
            stopwords = lines.collect(Collectors.toList());
            stopwords.add("");
        } catch (IOException | URISyntaxException e) {
            System.out.println("Error in readStopWords() for fileName=" + fileName);
        }
        return stopwords;
    }

    public static List<String> readAllFilesFromDirectory(String directoryName) {
        List<String> text;
        try {
            Path configFilePath = getResorcesPath(directoryName);
            text = readAllFilesFromPath(directoryName, configFilePath);
        } catch (IOException | URISyntaxException e) {
            System.out.println("Error in readAllFilesFromDirectory() for " + directoryName);
            text = emptyList();
        }
        return text;
    }

    private static Path getResorcesPath(String directoryName) {
        String resorcePath = Objects.requireNonNull(WordFrequencyJava.class.getClassLoader().getResource(directoryName)).getPath();
        return FileSystems.getDefault()
                .getPath(resorcePath);
    }

    private static List<String> readAllFilesFromPath(String directoryName, Path configFilePath) throws IOException, URISyntaxException {
        List<Path> collect = Files.walk(configFilePath)
                .collect(Collectors.toList());

        return Files.walk(configFilePath)
                .filter(s -> s.toString().endsWith(".txt"))
                .map(Utils::readFromFile)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private static List<String> readFromFile(Path pathToFile) {
        try (Stream<String> lines = Files.lines(pathToFile)) {
            return lines.collect(Collectors.toList());
        } catch (IOException e) {
            System.out.println("Error in readFromFile() for " + pathToFile.toString());
            return Collections.emptyList();
        }
    }
}

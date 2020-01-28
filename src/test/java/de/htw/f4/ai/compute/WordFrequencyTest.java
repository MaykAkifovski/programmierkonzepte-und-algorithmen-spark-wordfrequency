package de.htw.f4.ai.compute;

import de.htw.f4.ai.result.Result;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class WordFrequencyTest {

    private static final List<String> TEST_BOOK = Utils.readAllFilesFromDirectory("english_runtime_tests/test_book.txt");
    private static final List<String> STOPWORDS = Utils.readStopwords("english");
    private static final String[] WORDS = {
            "tom", "huck", "dont", "time", "boys", "joe", "aint", "boy", "began", "ill"
    };
    private static final Long[] WORDS_COUNT = {
            696L, 225L, 223L, 191L, 169L, 133L, 123L, 122L, 110L, 108L
    };

    @Test
    public void testJava() {
        Result result = WordFrequencyJava.run(TEST_BOOK, STOPWORDS);
        assertResultIsOkay(result);
    }

    @Test
    public void testJavaParallel() {
        Result result = WordFrequencyJavaParallel.run(TEST_BOOK, STOPWORDS);
        assertResultIsOkay(result);
    }

    @Test
    public void testSpark() {
        Result result;

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            result = WordFrequencySpark.run(TEST_BOOK, STOPWORDS, 4, jsc);
        }
        assertResultIsOkay(result);
    }


    private void assertResultIsOkay(Result result) {
        assertNotNull(result);
        assertTrue(result.wordsCountInCorpus == 25265);
        assertTop10Words(result.top10Words);
    }

    private void assertTop10Words(List<Map.Entry<String, Long>> top10Words) {
        Object[] words = top10Words.stream().map(Map.Entry::getKey).toArray();
        Object[] wordsCount = top10Words.stream().map(Map.Entry::getValue).toArray();

        assertArrayEquals(WORDS, words);
        assertArrayEquals(WORDS_COUNT, wordsCount);
    }

}

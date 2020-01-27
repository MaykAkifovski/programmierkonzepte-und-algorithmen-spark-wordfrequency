package de.htw.f4.ai.compute;

import de.htw.f4.ai.result.Result;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class WordFrequencyTest {

    private static final String TEST_BOOK = "test_book.txt";
    private static final String[] WORDS = {
            "tom", "huck", "don", "time", "joe", "boys", "boy", "good", "becky", "began"
    };
    private static final Long[] WORDS_COUNT = {
            786L, 251L, 224L, 191L, 164L, 159L, 135L, 113L, 112L, 110L
    };

    @Test
    public void testJava() {
        Result result = WordFrequencyJava.run(TEST_BOOK);
        assertResultIsOkay(result);
    }

    @Test
    public void testJavaParallel() {
        Result result = WordFrequencyJavaParallel.run(TEST_BOOK);
        assertResultIsOkay(result);
    }

    @Test
    public void testSpark() {
        Result result;

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            result = WordFrequencySpark.run(TEST_BOOK, 4, jsc);
        }
        assertResultIsOkay(result);
    }


    private void assertResultIsOkay(Result result) {
        assertNotNull(result);
        assertTrue(result.wordsCountInCorpus == 25324);
        assertTop10Words(result.top10Words);
    }

    private void assertTop10Words(List<Map.Entry<String, Long>> top10Words) {
        Object[] words = top10Words.stream().map(Map.Entry::getKey).toArray();
        Object[] wordsCount = top10Words.stream().map(Map.Entry::getValue).toArray();

        assertArrayEquals(WORDS, words);
        assertArrayEquals(WORDS_COUNT, wordsCount);
    }

}

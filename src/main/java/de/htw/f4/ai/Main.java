package de.htw.f4.ai;

import de.htw.f4.ai.compute.WordFrequencyJava;
import de.htw.f4.ai.compute.WordFrequencyJavaParallel;
import de.htw.f4.ai.compute.WordFrequencySpark;
import de.htw.f4.ai.result.Result;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

    private static final String FILE = "one_book.txt";
    private static final String FILE_2 = "two_books.txt";
    private static final String FILE_4 = "four_books.txt";
    private static final String FILE_ALL = "all_books.txt";

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {

            Result dumy = WordFrequencySpark.run(FILE, 2, jsc);

            // (<Top10Words and Count>, WordsCountInCorpus, TimeToCompute)
            Result result = WordFrequencySpark.run(FILE, 2, jsc);
            WordFrequencySpark.run(FILE, 4, jsc);
            WordFrequencySpark.run(FILE, 8, jsc);
            WordFrequencySpark.run(FILE, 16, jsc);

            WordFrequencySpark.run(FILE_2, 2, jsc);
            WordFrequencySpark.run(FILE_2, 4, jsc);
            WordFrequencySpark.run(FILE_2, 8, jsc);
            WordFrequencySpark.run(FILE_2, 16, jsc);

            WordFrequencySpark.run(FILE_4, 2, jsc);
            WordFrequencySpark.run(FILE_4, 4, jsc);
            WordFrequencySpark.run(FILE_4, 8, jsc);
            WordFrequencySpark.run(FILE_4, 16, jsc);

            WordFrequencySpark.run(FILE_ALL, 2, jsc);
            WordFrequencySpark.run(FILE_ALL, 4, jsc);
            WordFrequencySpark.run(FILE_ALL, 8, jsc);
            WordFrequencySpark.run(FILE_ALL, 16, jsc);

            jsc.stop();
        }

        WordFrequencyJava.run(FILE);
        WordFrequencyJava.run(FILE_2);
        WordFrequencyJava.run(FILE_4);
        WordFrequencyJava.run(FILE_ALL);

        WordFrequencyJavaParallel.run(FILE);
        WordFrequencyJavaParallel.run(FILE_2);
        WordFrequencyJavaParallel.run(FILE_4);
        WordFrequencyJavaParallel.run(FILE_ALL);
    }
}

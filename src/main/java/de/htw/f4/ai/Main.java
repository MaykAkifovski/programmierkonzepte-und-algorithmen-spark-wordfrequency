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

            Result dummy = WordFrequencySpark.run(FILE, 2, jsc);

            // (<Top10Words and Count>, WordsCountInCorpus, TimeToCompute)
            Result resultBooks1SparkPart2 = WordFrequencySpark.run(FILE, 2, jsc);
            Result resultBooks1SparkPart4 = WordFrequencySpark.run(FILE, 4, jsc);
            Result resultBooks1SparkPart8 = WordFrequencySpark.run(FILE, 8, jsc);
            Result resultBooks1SparkPart16 = WordFrequencySpark.run(FILE, 16, jsc);
            Result resultBooks1SparkPart32 = WordFrequencySpark.run(FILE, 32, jsc);

            Result resultBooks2SparkPart2 = WordFrequencySpark.run(FILE_2, 2, jsc);
            Result resultBooks2SparkPart4 = WordFrequencySpark.run(FILE_2, 4, jsc);
            Result resultBooks2SparkPart8 = WordFrequencySpark.run(FILE_2, 8, jsc);
            Result resultBooks2SparkPart16 = WordFrequencySpark.run(FILE_2, 16, jsc);
            Result resultBooks2SparkPart32 =  WordFrequencySpark.run(FILE_2, 32, jsc);

            Result resultBooks4SparkPart2 = WordFrequencySpark.run(FILE_4, 2, jsc);
            Result resultBooks4SparkPart4 = WordFrequencySpark.run(FILE_4, 4, jsc);
            Result resultBooks4SparkPart8 = WordFrequencySpark.run(FILE_4, 8, jsc);
            Result resultBooks4SparkPart16 = WordFrequencySpark.run(FILE_4, 16, jsc);
            Result resultBooks4SparkPart32 = WordFrequencySpark.run(FILE_4, 32, jsc);

            Result resultBooks52SparkPart2  = WordFrequencySpark.run(FILE_ALL, 2, jsc);
            Result resultBooks52SparkPart4  = WordFrequencySpark.run(FILE_ALL, 4, jsc);
            Result resultBooks52SparkPart8  = WordFrequencySpark.run(FILE_ALL, 8, jsc);
            Result resultBooks52SparkPart16 = WordFrequencySpark.run(FILE_ALL, 16, jsc);
            Result resultBooks52SparkPart32 = WordFrequencySpark.run(FILE_ALL, 32, jsc);

            System.out.println("top 10 words in 52 english books: " + resultBooks52SparkPart8.top10Words);
            System.out.println("total number of words: " + resultBooks52SparkPart8.wordsCountInCorpus + "");
            System.out.println("compute duration (Spark - 2 partitions): " + resultBooks52SparkPart2.timeToComputeMs + " ms");
            System.out.println("compute duration (Spark - 4 partitions): " + resultBooks52SparkPart4.timeToComputeMs + " ms");
            System.out.println("compute duration (Spark - 8 partitions): " + resultBooks52SparkPart8.timeToComputeMs + " ms");
            System.out.println("compute duration (Spark - 16 partitions): " + resultBooks52SparkPart16.timeToComputeMs + " ms");
            System.out.println("compute duration (Spark - 32 partitions): " + resultBooks52SparkPart32.timeToComputeMs + " ms");

            jsc.stop();
        }

        Result resultBooks1Java = WordFrequencyJava.run(FILE);
        Result resultBooks2Java = WordFrequencyJava.run(FILE_2);
        Result resultBooks4Java = WordFrequencyJava.run(FILE_4);
        Result resultBooks52Java = WordFrequencyJava.run(FILE_ALL);

        Result resultBooks1JavaParallel = WordFrequencyJavaParallel.run(FILE);
        Result resultBooks2JavaParallel = WordFrequencyJavaParallel.run(FILE_2);
        Result resultBooks4JavaParallel = WordFrequencyJavaParallel.run(FILE_4);
        Result resultBooks52JavaParallel = WordFrequencyJavaParallel.run(FILE_ALL);

        System.out.println("compute duration (Java): " + resultBooks52Java.timeToComputeMs + " ms");
        System.out.println("compute duration (Java - parallel): " + resultBooks52JavaParallel.timeToComputeMs + " ms");
    }
}

package de.htw.f4.ai.result;

import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Result {

    public List<Map.Entry<String, Long>> top10Words;
    public Long wordsCountInCorpus;
    public Long timeToComputeMs;

    public Result(List<Map.Entry<String, Long>> top10Words, Long wordsCountInCorpus, Long timeToComputeMs) {
        this.top10Words = top10Words;
        this.wordsCountInCorpus = wordsCountInCorpus;
        this.timeToComputeMs = timeToComputeMs;
    }

    private Result() {
    }

    public Result(List<Tuple2<String, Integer>> top10Words, long wordsCountInCorpus, long timeToComputeMs) {
        this.top10Words = top10Words.stream().map(stringIntegerTuple2 -> Map.entry(stringIntegerTuple2._1, (long) stringIntegerTuple2._2)).collect(Collectors.toList());
        this.wordsCountInCorpus = wordsCountInCorpus;
        this.timeToComputeMs = timeToComputeMs;
    }
}

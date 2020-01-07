package de.htw.f4.ai;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Main {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        MainSpark.main(new String[]{"2"});
        MainSpark.main(new String[]{"4"});
        MainSpark.main(new String[]{"8"});
        MainSpark.main(new String[]{"16"});
        MainJava.main(null);
        MainJavaParallel.main(null);
    }
}

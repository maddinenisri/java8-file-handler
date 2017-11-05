package com.mdstech.largefile.stream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.mdstech.largefile.stream.FixedBatchCustomIterator.withBatchSize;

/**
 * Created by srini on 5/23/17.
 */
public class ReadFileDataAsStream {

    private final Path inputFilePath;

    public ReadFileDataAsStream(String inputCsvLocation) {
        this.inputFilePath = Paths.get(inputCsvLocation);
    }

    public Stream<String> readDataStreamFromFile(int batchSize) throws IOException {
        return withBatchSize(Files.lines(inputFilePath), batchSize);
    }
}

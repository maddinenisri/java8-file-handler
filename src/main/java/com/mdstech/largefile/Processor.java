package com.mdstech.largefile;

import com.mdstech.largefile.stream.ReadFileDataAsStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.*;

public class Processor {

    public void processLargeFile(String largeFilePath) throws Exception {
//        ExecutorService executor = Executors.newFixedThreadPool(100);
        Instant startInstant = Instant.now();
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "100");
        ReadFileDataAsStream readFileDataAsStream = new ReadFileDataAsStream(largeFilePath);

        List<CompletableFuture<String>> batches =
                BatchingIterator.batchedStreamOf(readFileDataAsStream.readDataStreamFromFile(5000), 5000)
                    .map(list -> processChunk(list))
                    .collect(Collectors.<CompletableFuture<String>>toList());

        CompletableFuture<Void> allDoneFuture =
                CompletableFuture.allOf(batches.toArray(new CompletableFuture[batches.size()]));

        CompletableFuture<List<String>> filenames =
                allDoneFuture
                        .thenApply(v ->
                                batches
                                        .stream()
                                        . map(future -> future.join())
                                        .collect(Collectors.<String>toList()));

        filenames.thenAcceptAsync( this::combineFiles ).get();
        Instant endInstant = Instant.now();
        System.out.println("elapsed time ( milliseconds ): " + Duration.between(startInstant, endInstant).toMillis());
    }

    private void combineFiles(List<String> files) {
        Instant startInstant = Instant.now();
        System.out.println("Start combine files");
        Path path = Paths.get("/Users/srini/IdeaProjects/java8-file-handler/target/output_data.csv");
        try(FileChannel fileChannel = FileChannel.open(
                path, WRITE, CREATE)) {
            files.stream().forEach(fileName -> {
                Path inPath = Paths.get(String.format("/Users/srini/IdeaProjects/java8-file-handler/target/data_%s.csv", fileName));
                try(FileChannel inFileChannel = FileChannel.open(
                        inPath, READ)) {
                    try {
                        for(long p = 0, l = inFileChannel.size(); p<l; ) {
                            p+=inFileChannel.transferTo(p, l-p, fileChannel);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    ByteBuffer buffer = ByteBuffer.allocate(1);
                    buffer.put(System.lineSeparator().getBytes());
                    fileChannel.write(buffer);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            ByteBuffer buffer = ByteBuffer.allocate(1);
            buffer.put(System.lineSeparator().getBytes());
            fileChannel.write(buffer);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        Instant endInstant = Instant.now();
        System.out.println("End combine files, total time in millis: " + Duration.between(startInstant, endInstant).toMillis());
    }

    private CompletableFuture<String> processChunk(List<String> chunkData) {
        return CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                String fileName = null;
                try {
                    fileName = writeToFile(chunkData);
                    System.out.println("Processing...."+Thread.currentThread().getName()+" .... " + chunkData.size());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return fileName;
            }
        });
    }

    private String writeToFile(List<String> chunkData) throws IOException {
        String fileName = UUID.randomUUID().toString();
        Path path = Paths.get(String.format("/Users/srini/IdeaProjects/java8-file-handler/target/data_%s.csv", fileName));
        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(
                path, WRITE, CREATE);

        ByteBuffer buffer = ByteBuffer.allocate(chunkData.size()*2048);
        String data = chunkData.stream().collect(Collectors.joining(System.lineSeparator())).concat(System.lineSeparator());
        buffer.put(data.getBytes());

        buffer.flip();

        fileChannel.write(
                buffer, 0, buffer, new CompletionHandler<Integer, ByteBuffer>() {
                    @Override
                    public void completed(Integer result, ByteBuffer attachment) {
                        System.out.println("Attachment: " + attachment + " " + result
                                + " bytes written");
                        System.out.println("CompletionHandler Thread ID: "
                                + Thread.currentThread().getId());
                    }
                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment) {
                        System.err.println("Attachment: " + attachment + " failed with:");
                        exc.printStackTrace();
                    }
                });
        return fileName;
    }
}

package com.mdstech.largefile;

import com.aol.cyclops2.types.futurestream.LazyFutureStreamFunctions;
import com.mdstech.largefile.bus.BlockingQueueContainer;
import com.mdstech.largefile.stream.ReadFileDataAsStream;
import com.mdstech.largefile.util.FunctionWithException;
import cyclops.async.LazyReact;
import cyclops.async.adapters.Queue;
import cyclops.collections.mutable.ListX;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingByConcurrent;

public class Processor {

    public void processLargeFile(String largeFilePath) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        BlockingQueueContainer blockingQueueContainer = new BlockingQueueContainer(new LinkedBlockingDeque<>());
        ReadFileDataAsStream readFileDataAsStream = new ReadFileDataAsStream(largeFilePath);

        List<CompletableFuture<Boolean>> batches =
                BatchingIterator.batchedStreamOf(readFileDataAsStream.readDataStreamFromFile(25), 5000)
                    .map(list -> processChunk(list, executor))
                    .collect(Collectors.<CompletableFuture<Boolean>>toList());


        CompletableFuture<Void> allDoneFuture =
                CompletableFuture.allOf(batches.toArray(new CompletableFuture[batches.size()]));

        CompletableFuture<List<Boolean>> statuses =
                allDoneFuture
                        .thenApply(v ->
                                batches
                                        .stream()
                                        . map(future -> future.join())
                                        .collect(Collectors.<Boolean>toList()));

        System.out.println(statuses.get().size());



//        CompletableFuture aComFuture = CompletableFuture.supplyAsync(() -> "", executor);
//        BatchingIterator.batchedStreamOf(readFileDataAsStream.readDataStreamFromFile(25), 5000)
//                .forEach(list -> aComFuture.thenApply(fn -> process(list)));
//
//        aComFuture.join();

//        Long num = BatchingIterator.batchedStreamOf(readFileDataAsStream.readDataStreamFromFile(25), 5000)
////                .map(list -> {
////                    Collections.shuffle(list); return list; })
////                .flatMap(List::stream)
//                .map(x -> process(x)).collect(counting());

//        System.out.println(num);
//        LazyReact.parallelBuilder().fromStream(readFileDataAsStream.readDataStreamFromFile(25))
//                .grouped(5000)
//                .map(this::process)
//                .run();


//        Queue queue = new Queue();
//
//        CompletableFuture<Integer> integerCompletableFuture = new CompletableFuture<>();
//        Runnable runnable1 = new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    queue.fromStream(readFileDataAsStream.readDataStreamFromFile(25));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//
//        Runnable runnable2 = () -> {
//            queue.stream()
//                    .map(entity -> true)
//                    .collect(groupingByConcurrent(s -> s, counting()));
//        };
//
//        CompletableFuture.runAsync(runnable1).thenCombine(runnable2)

        //        Map<Object, Long> words  = readFileDataAsStream
//                .readDataStreamFromFile(25).parallel()
//                .map(line -> blockingQueueContainer.putToQueue(line))
//                .collect(groupingByConcurrent(s -> s, counting()));;
//
//        CompletableFuture.supplyAsync(() -> blockingQueueContainer.takeFromQueue(), executor);
//        System.out.println("----------");
//
//
//        blockingQueueContainer.setNoData(true);
//        System.out.println(words +":"+ blockingQueueContainer.getQueueSize());
////        while(!completableFuture.isDone()) {
////            System.out.println("Waiting for completion ..."+ blockingQueueContainer.getQueueSize());
////        }
//        System.out.println("Completed reading....");
    }

    private Integer process(List<String> strings) {
        System.out.println("Processing...."+Thread.currentThread().getName());
        return 1;
    }



//    private String process(ListX<String> strings) {
//        System.out.println("Processing....");
//        return null;
//    }


    private <T, R, E extends Exception> Function<T, R> wrapper(FunctionWithException<T, R, E> fe) {
        return arg -> {
            try {
                return fe.apply(arg);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private String parseData(String status) {
        System.out.println("Processing...."+Thread.currentThread().getName());
        return status;
    }

    private String writeToFile(List<String> data) {
        try {

            Thread.sleep(1000);
            System.out.println("Processing...."+Thread.currentThread().getName()+" .... " + data.get(0));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "success";
    }

    private CompletableFuture<Boolean> processChunk(List<String> chunkData, ExecutorService executor) {
        return CompletableFuture.supplyAsync(new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                try {
                    Thread.sleep(1000);
                    System.out.println("Processing...."+Thread.currentThread().getName()+" .... " + chunkData.size());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return false;
                }
                return true;
            }
        }, executor);
    }

}

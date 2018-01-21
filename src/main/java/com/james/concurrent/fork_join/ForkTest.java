package com.james.concurrent.fork_join;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;

public class ForkTest {
    public static void main(String[] args) {
        /*
         * An ExecutorService for running ForkJoinTasks. A ForkJoinPool provides the
         * entry point for submissions from non-ForkJoinTask clients, as well as
         * management and monitoring operations.
         * 
         * A ForkJoinPool differs from other kinds of ExecutorService mainly by virtue
         * of employing work-stealing: all threads in the pool attempt to find and
         * execute tasks submitted to the pool and/or created by other active tasks
         * (eventually blocking waiting for work if none exist). This enables efficient
         * processing when most tasks spawn other subtasks (as do most ForkJoinTasks),
         * as well as when many small tasks are submitted to the pool from external
         * clients. Especially when setting asyncMode to true in constructors,
         * ForkJoinPools may also be appropriate for use with event-style tasks that are
         * never joined.
         */
        ForkJoinPool pool = new ForkJoinPool();

        FolderProcessor system = new FolderProcessor("/Users/qjiang/workspace/all-in-one", "java");
        FolderProcessor apps = new FolderProcessor("/Users/qjiang/workspace/all-in-one", "md");
        FolderProcessor documents = new FolderProcessor("/Users/qjiang/workspace/all-in-one", "xml");

        pool.execute(system);
        pool.execute(apps);
        pool.execute(documents);

        do {
            System.out.printf("*************************************************************\n");
            System.out.printf("Main: Parallelism: %d\n", pool.getParallelism());
            System.out.printf("Main: Active Threads: %d\n", pool.getActiveThreadCount());
            System.out.printf("Main: Task Count: %d\n", pool.getQueuedTaskCount());
            System.out.printf("Main: Steal Count: %d\n", pool.getStealCount());
            System.out.printf("*************************************************************\n\n");

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while ((!system.isDone()) || (!apps.isDone()) || (!documents.isDone()));

        pool.shutdown();

        List<String> results;
        results = system.join();
        System.out.printf("System: %d files found.", results.size());
        results = apps.join();
        System.out.printf("Apps: %d files found.", results.size());
        results = documents.join();
        System.out.printf("Documents: %d files found.", results.size());
    }
}

class FolderProcessor extends RecursiveTask<List<String>> {
    /**
     * 
     */
    private static final long serialVersionUID = 8269191946020127581L;
    private String path;
    private String extension;

    public FolderProcessor(String path, String extension) {
        this.path = path;
        this.extension = extension;
    }

    @Override
    protected List<String> compute() {
        List<String> list = new ArrayList<String>();
        List<FolderProcessor> tasks = new ArrayList<>();

        File file = new File(path);
        File[] content = file.listFiles();

        if (content != null) {
            for (int i = 0; i < content.length; i++) {
                if (content[i].isDirectory()) {
                    FolderProcessor task = new FolderProcessor(content[i].getAbsolutePath(), extension);

                    /*
                     * Arranges to asynchronously execute this task in the pool the current task is
                     * running in, if applicable, or using the ForkJoinPool.commonPool() if not
                     * inForkJoinPool. While it is not necessarily enforced, it is a usage error to
                     * fork a task more than once unless it has completed and been reinitialized.
                     * Subsequent modifications to the state of this task or any data it operates on
                     * are not necessarily consistently observable by any thread other than the one
                     * executing it unless preceded by a call to join or related methods, or a call
                     * to isDone returning true.
                     */
                    task.fork();

                    /*
                     * Appends the specified element to the end of this list (optional operation).
                     * 
                     * Lists that support this operation may place limitations on what elements may
                     * be added to this list. In particular, some lists will refuse to add null
                     * elements, and others will impose restrictions on the type of elements that
                     * may be added. List classes should clearly specify in their documentation any
                     * restrictions on what elements may be added.
                     */
                    tasks.add(task);
                } else {
                    if (checkFile(content[i].getName())) {
                        list.add(content[i].getAbsolutePath());
                    }
                }
            }

            if (tasks.size() > 50) {
                System.out.printf("%s: %d tasks ran.\n", file.getAbsolutePath(), tasks.size());
            }

            addResultsFromTasks(list, tasks);
        }
        return list;
    }

    private void addResultsFromTasks(List<String> list, List<FolderProcessor> tasks) {
        for (FolderProcessor item : tasks) {
            /*
             * Returns the result of the computation when it is done. This method differs
             * from get() in that abnormal completion results in RuntimeException or Error,
             * not ExecutionException, and that interrupts of the calling thread do not
             * cause the method to abruptly return by throwing InterruptedException.
             */
            list.addAll(item.join());
        }
    }

    private boolean checkFile(String name) {
        return name.endsWith(extension);
    }
}

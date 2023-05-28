import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import mpi.*;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        MPI.Init(args);
//        Graph graph1 = new Graph();
//        graph1.addNode(2, 2, 1, 9, 4); // 0
//        graph1.addNode(4, 4, 8); // 1
//        graph1.addNode(8, 1, 6, 9, 2, 6); // 2
//        graph1.addNode(1, 3, 8, 3);       // 3
//        graph1.addNode();    // 4
//        graph1.addNode();       // 5
//        graph1.addNode(5, 1);       // 6
//        graph1.addNode(7, 9, 3, 6);       // 7
//        graph1.addNode(0);       // 8
//        graph1.addNode(1, 8, 0, 5);       // 9
//      System.out.println(graph1.serialBFS());
        int[][] times = new int[6][30];
        Graph graph1 = new Graph(3000);
        //System.out.println(graph1);
        long time = 0;
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        if (rank == 0) {
            time = Calendar.getInstance().getTimeInMillis();
        }
        graph1.MPIBFS();
        if (rank == 0) {
            times[0][0] = (int) (Calendar.getInstance().getTimeInMillis() - time);
            System.out.println(Arrays.stream(times[0]).sum());
        }
        TimeUnit.SECONDS.sleep(1);
        if (rank == 0) {
            for (int i = 0; i < 1; i++) {

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(1);
                times[1][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println("Time spent with 1 thread: " + Arrays.stream(times[1]).sum() + "ms");
                TimeUnit.SECONDS.sleep(1);

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(2);
                times[2][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println("Time spent with 2 threads: " + Arrays.stream(times[2]).sum() + "ms");
                TimeUnit.SECONDS.sleep(1);

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(3);
                times[3][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println("Time spent with 3 threads: " + Arrays.stream(times[3]).sum() + "ms");
                TimeUnit.SECONDS.sleep(1);

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(4);
                times[4][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println("Time spent with 4 threads: " + Arrays.stream(times[4]).sum() + "ms");
                TimeUnit.SECONDS.sleep(1);

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(5);
                times[5][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println("Time spent with 5 threads: " + Arrays.stream(times[5]).sum() + "ms");
                TimeUnit.SECONDS.sleep(1);

                //System.out.println(Arrays.stream(times[0]).sum() + " ms");
//            System.out.println(Arrays.stream(times[1]).sum() + " ms");
//            System.out.println(Arrays.stream(times[2]).sum() + " ms");
//            System.out.println(Arrays.stream(times[3]).sum() + " ms");
//            System.out.println(Arrays.stream(times[4]).sum() + " ms");
//            System.out.println(Arrays.stream(times[5]).sum() + " ms");

                System.out.println(graph1.isParallelDistancesEqual());
            }
        }
        MPI.Finalize();
    }
}

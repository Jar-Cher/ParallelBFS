import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {
//        Graph graph1 = new Graph();
//        graph1.addNode(7, 8); // 0
//        graph1.addNode(0, 2); // 1
//        graph1.addNode(4, 9, 9); // 2
//        graph1.addNode(7, 3, 4);       // 3
//        graph1.addNode(6, 3, 2, 1, 1);    // 4
//        graph1.addNode(6, 4, 5);       // 5
//        graph1.addNode();       // 6
//        graph1.addNode(8, 9, 1, 5);       // 7
//        graph1.addNode();       // 8
//        graph1.addNode();       // 9

        for (int j = 0; j < 20; j++) {
            //System.out.println(graph.serialBFS());
            int[][] times = new int[6][30];
            Graph graph1 = new Graph(10000);
            long time;
            for (int i = 0; i < 1; i++) {

                //time = Calendar.getInstance().getTimeInMillis();
                //graph1.serialBFS();
                //times[0][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                //System.out.println(Arrays.stream(times[0]).sum());

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(1);
                times[1][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println(Arrays.stream(times[1]).sum());
                TimeUnit.SECONDS.sleep(1);

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(2);
                times[2][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println(Arrays.stream(times[2]).sum());
                TimeUnit.SECONDS.sleep(1);

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(3);
                times[3][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println(Arrays.stream(times[3]).sum());
                TimeUnit.SECONDS.sleep(1);

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(4);
                times[4][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println(Arrays.stream(times[4]).sum());
                TimeUnit.SECONDS.sleep(1);

                time = Calendar.getInstance().getTimeInMillis();
                graph1.parallelBFS(5);
                times[5][i] = (int) (Calendar.getInstance().getTimeInMillis() - time);
                System.out.println(Arrays.stream(times[5]).sum());
                TimeUnit.SECONDS.sleep(1);
            }
            //System.out.println(Arrays.stream(times[0]).sum() + " ms");
//            System.out.println(Arrays.stream(times[1]).sum() + " ms");
//            System.out.println(Arrays.stream(times[2]).sum() + " ms");
//            System.out.println(Arrays.stream(times[3]).sum() + " ms");
//            System.out.println(Arrays.stream(times[4]).sum() + " ms");
//            System.out.println(Arrays.stream(times[5]).sum() + " ms");
            //System.out.println(graph1);
            System.out.println(graph1.isParallelDistancesEqual());
        }
    }
}

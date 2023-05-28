import mpi.MPI;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Graph implements Iterable<Integer> {
    private Integer startNodeId = null;
    private int processorsReserved = 6;
    private final Deque<Integer> startingQueue = new ConcurrentLinkedDeque<>();

    public int size = 0;

    public int[] localNodes;

    public int[] adjMatrix;

    public List<Node> nodes = new CopyOnWriteArrayList<>();

    private final Deque<Integer> queue = new ConcurrentLinkedDeque<Integer>();
    private final Set<Integer> visitedNodes = new CopyOnWriteArraySet<Integer>();

    public Graph() {
    }

    public Graph(int nodesAmount) {
        this(nodesAmount, 6);
    }

    public Graph(int nodesAmount, int processorsReserved) {
        Random random = new Random();
        this.processorsReserved = processorsReserved;
        int rank = MPI.COMM_WORLD.Rank();
        for (int i = 0; i < nodesAmount; i++) {
            //List<Integer> adjNodes = new ArrayList<>();
            int[] adjNodesArray = new int[nodesAmount];
            int amountAdjNodes = nodesAmount;
            if (rank == 0) {
                amountAdjNodes = random.nextInt(nodesAmount);
                adjNodesArray = new int[amountAdjNodes];

                for (int j = 0; j < amountAdjNodes; j++) {
                    int newNode = random.nextInt(nodesAmount);
                    adjNodesArray[j] = newNode;
                }
            }
            MPI.COMM_WORLD.Barrier();
            MPI.COMM_WORLD.Bcast(adjNodesArray, 0, amountAdjNodes, MPI.INT, 0);
            //System.out.println(Arrays.toString(adjNodesArray));
            this.addNode(adjNodesArray);
        }
        localNodes = new int[this.size()];
        adjMatrix = new int[this.size() * this.size()];
        for (int i = 0; i < this.size(); i++) {
            localNodes[i] = i;
            for (int j = 0; j < nodes.get(i).adj.size(); j++) {
                adjMatrix[i * this.size() + nodes.get(i).adj.get(j)] = 1;
            }
        }
    }

    public int getStartNodeId() {
        return startNodeId;
    }

    public void setStartNodeId(int startNodeId) {
        nodes.get(this.startNodeId).serialDistance = Integer.MAX_VALUE;
        for (int i = 0; i < nodes.get(this.startNodeId).parallelDistance.size(); i++) {
            nodes.get(this.startNodeId).parallelDistance.set(i, Integer.MAX_VALUE);
        }
        this.startNodeId = startNodeId;
        startingQueue.pop();
        startingQueue.add(startNodeId);
        nodes.get(this.startNodeId).serialDistance = 0;
        for (int i = 0; i < nodes.get(this.startNodeId).parallelDistance.size(); i++) {
            nodes.get(this.startNodeId).parallelDistance.set(i, 0);
        }
    }

    public int size() {
        return this.size;
    }

    public void addNode(List<Integer> adj) {
        Node node = new Node(size(), adj);
        if (size() == 0) {
            startNodeId = 0;
            node.serialDistance = 0;
            for (int i = 0; i < processorsReserved; i++) {
                node.parallelDistance.set(i, 0);
            }
            startingQueue.add(startNodeId);
        }
        nodes.add(node);
        size++;
    }

    public void addNode(int... adj) {
        Node node = new Node(size(), adj);
        if (size() == 0) {
            startNodeId = 0;
            node.serialDistance = 0;
            for (int i = 0; i < processorsReserved; i++) {
                node.parallelDistance.set(i, 0);
            }
            startingQueue.add(startNodeId);
        }
        nodes.add(node);
        size++;
    }

    public boolean isParallelDistancesEqual() {
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = 1; j < processorsReserved; j++) {
                if (!nodes.get(i).parallelDistance.get(j - 1).equals(nodes.get(i).parallelDistance.get(j))) {
                    System.out.println(nodes.get(i));
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "Graph{" +
                "startNodeId=" + startNodeId +
                ", startingQueue=" + startingQueue +
                ", nodes=" + nodes +
                '}';
    }

    public String serialBFS() {
        StringBuilder str = new StringBuilder();
        for (Integer i : this) {
            str.append(i).append(" ");
        }
        return str.toString();
    }

    @Override
    public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {

            final Deque<Integer> queue = new ArrayDeque<Integer>(startingQueue);
            final Set<Integer> visitedNodes = new HashSet<Integer>();

            @Override
            public boolean hasNext() {
                if (queue.isEmpty()) {
                    return false;
                }
                while (visitedNodes.contains(queue.getFirst())) {
                    queue.remove();
                    if (queue.isEmpty()) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Integer next() {
                int next = queue.remove();
                while (visitedNodes.contains(next)) {
                    next = queue.remove();
                }
                for (Integer i : nodes.get(next).adj) {
                    queue.add(i);
                    nodes.get(i).serialDistance = Integer.min(nodes.get(next).serialDistance + 1,
                            nodes.get(i).serialDistance);
                }
                visitedNodes.add(next);
                return next;
            }
        };
    }

    public void MPIBFS() {
        long time = Calendar.getInstance().getTimeInMillis();
        long fulltime = Calendar.getInstance().getTimeInMillis();
        long[] totalTimes = new long[6];
        int[] MPIdistances = IntStream.range(0, this.size()).map(it -> Integer.MAX_VALUE).toArray();
        int rank = MPI.COMM_WORLD.Rank();
        int processorsAmount = MPI.COMM_WORLD.Size();
        //System.out.println("From <"+rank+">: <"+size+">");
        int level = 0;
        int served = 0;
        int[] nextFrontier = new int[this.size()];
        nextFrontier[startingQueue.getFirst()] = 1;
        int[] visited = new int[this.size()];

        totalTimes[0] = totalTimes[0] + Calendar.getInstance().getTimeInMillis() - time;
        //System.out.println("Processor #<"+rank+"> adjMatrix is <"+ Arrays.toString(adjMatrix) +">");
        //System.out.println("Processor #<"+rank+"> localNodes is <"+ Arrays.toString(localNodes) +">");

        time = Calendar.getInstance().getTimeInMillis();
        while (Arrays.stream(nextFrontier).anyMatch(it -> it == 1)) {
            int chunk = this.size() / processorsAmount + this.size() % processorsAmount;
            int[] currentFrontier = new int[chunk];
            if ((rank + 1) * chunk - rank * chunk >= 0)
                System.arraycopy(nextFrontier, rank * chunk, currentFrontier, 0, Integer.min(chunk, this.size() - rank * chunk));
            nextFrontier = new int[this.size()];
            //System.out.println("Process <"+rank+"> have frontier of <"+ Arrays.toString(currentFrontier) +">");

            Set<Integer> currentFrontierSet = new HashSet<>();
            for (int i = 0; i < currentFrontier.length; i ++) {
                if (currentFrontier[i] == 1) {
                    currentFrontierSet.add(i);
                }
            }
            totalTimes[1] = totalTimes[1] + Calendar.getInstance().getTimeInMillis() - time;
            //System.out.println("Process <"+rank+"> have frontier of size <"+ currentFrontierSet.size() +">");
            time = Calendar.getInstance().getTimeInMillis();
            for (Integer i : currentFrontierSet) {
                int node = localNodes[i + rank * chunk];
                if (visited[node] == 0) {
                    //System.out.println("Processor <" + rank + "> now processes node #<" + node + ">");
                    MPIdistances[node] = level;
                    served++;
                    for (Integer j : nodes.get(node).adj) {
                        if (visited[j] == 0) {
                            nextFrontier[j] = 1;
                        }
                    }
                    visited[node] = 1;
                }
                nextFrontier[node] = 0;
            }
            totalTimes[2] = totalTimes[2] + Calendar.getInstance().getTimeInMillis() - time;
            time = Calendar.getInstance().getTimeInMillis();
            int[] displs = IntStream.range(0, processorsAmount).map(it -> it * this.size()).toArray();

            // MPI gather local visited sizes
            int[] nextVisitedSizes = IntStream.range(0, this.size()).map(it -> this.size()).toArray();
            // MPI gather visited
            int[] visitedArray = new int[processorsAmount * this.size()];
            //System.out.println("Processor <"+rank+"> reports visited following <"+ Arrays.toString(visitedArray) +"> nodes");
            MPI.COMM_WORLD.Allgatherv(visited, 0, visited.length, MPI.INT,
                    visitedArray, 0, nextVisitedSizes, displs, MPI.INT);
            //System.out.println("Processor <"+rank+"> reports visited following <"+ Arrays.toString(visitedArray) +"> nodes");
            for (int i = 0; i < processorsAmount; i ++) {
                for (int j = 0; j < this.size(); j ++) {
                    visited[j] = Integer.max(visited[j], visitedArray[i * this.size() + j]);
                }
            }
            totalTimes[3] = totalTimes[3] + Calendar.getInstance().getTimeInMillis() - time;
            //System.out.println("Processor <"+rank+"> folds visited as <"+ Arrays.toString(visited) +">");
            time = Calendar.getInstance().getTimeInMillis();

            // MPI gather next frontiers sizes
            int[] nextFrontierSizes = IntStream.range(0, this.size()).map(it -> this.size()).toArray();
//            System.out.println("Processor #"+rank+"> confirms sizes of new frontiers <"+ Arrays.toString(nextFrontierSizes) +">");

            // MPI gather next frontier
            int[] nextFrontierArray = new int[processorsAmount * this.size()];
            MPI.COMM_WORLD.Allgatherv(nextFrontier, 0, nextFrontier.length, MPI.INT,
                    nextFrontierArray, 0, nextFrontierSizes, displs, MPI.INT);
            //System.out.println("Processor #<"+rank+"> says full new frontiers <"+ Arrays.toString(nextFrontierArray) +">");
            for (int i = 0; i < processorsAmount; i ++) {
                for (int j = 0; j < this.size(); j ++) {
                    if (visited[j] == 0) {
                        nextFrontier[j] = Integer.max(nextFrontier[j], nextFrontierArray[i * this.size() + j]);
                    }
                }
            }
            totalTimes[4] = totalTimes[4] + Calendar.getInstance().getTimeInMillis() - time;
            //System.out.println("Processor #<"+rank+"> folds new frontier as <"+ Arrays.toString(nextFrontier) +">");
            time = Calendar.getInstance().getTimeInMillis();
            level++;
            //System.out.println("\nNext level: " + level);
        }
        int[] distancesArray = new int[processorsAmount * this.size()];
        MPI.COMM_WORLD.Allgather(MPIdistances, 0, MPIdistances.length, MPI.INT,
                distancesArray, 0, this.size(), MPI.INT);
        for (int i = 0; i < processorsAmount; i ++) {
            for (int j = 0; j < this.size(); j ++) {
                MPIdistances[j] = Integer.min(MPIdistances[j], distancesArray[i * this.size() + j]);
            }
        }
        if (rank == 0) {
            for (int i = 0; i < this.size(); i++) {
                nodes.get(i).parallelDistance.set(0, MPIdistances[i]);
            }
        }
        totalTimes[5] = totalTimes[5] + Calendar.getInstance().getTimeInMillis() - time;
        //System.out.println("Processor <"+rank+"> spent <"+ (Calendar.getInstance().getTimeInMillis() - fulltime) +">ms total");
        System.out.println("Processor <"+rank+"> wasted <"+ Arrays.toString(totalTimes) +">ms");
        //System.out.println("Processor <"+rank+"> served <"+ served +"> nodes");
    }

    public void parallelBFS(int processorsAmount) {

        final ArrayList<SearcherThread> threads = new ArrayList<>();
        visitedNodes.clear();
        queue.clear();
        queue.addAll(startingQueue);
        for (int i = 0; i < processorsAmount; i++) {
            SearcherThread thread = new SearcherThread(processorsAmount);
            threads.add(thread);
            thread.start();
        }
        while (threads.stream().anyMatch(thread -> !thread.isFree)) {

        }
        for (int i = 0; i < processorsAmount; i++) {
            threads.get(i).interrupt();

        }
        //System.out.println("Done");
    }

    private class SearcherThread extends Thread {
        private boolean isFree;
        private final int threadsUsed;

        public SearcherThread(int threadsUsed) {
            this.threadsUsed = threadsUsed;
        }

        @Override
        public void run() {
            while(!isInterrupted()) {
                if (!queue.isEmpty()) {
                    isFree = false;
                    Integer next;

                    while (visitedNodes.contains(queue.peek())) {
                        queue.poll();
                        if (queue.isEmpty()) {
                            isFree = true;
                        }
                    }
                    next = queue.poll();
                    if ((next == null) || (isFree)) {
                        isFree = true;
                        continue;
                    }
                    queue.addAll(nodes.get(next).adj
                            .stream().filter(it -> !visitedNodes.contains(it)).collect(Collectors.toSet()));
                    visitedNodes.add(next);
                    synchronized (nodes) {
                        for (Integer i : nodes.get(next).adj) {
                            nodes.get(i).parallelDistance.set(
                                    threadsUsed,
                                    Integer.min(nodes.get(next).parallelDistance.get(threadsUsed) + 1,
                                            nodes.get(i).parallelDistance.get(threadsUsed)));
                        }
                    }
                }
                else {
                    isFree = true;
                }
            }
        }

    }

    final private class Node {
        private final int id;
        private int serialDistance = Integer.MAX_VALUE;
        private final CopyOnWriteArrayList<Integer> parallelDistance = new CopyOnWriteArrayList<>();
        private List<Integer> adj = new ArrayList<>();

        public Node(int id, List<Integer> adj) {
            this.id = id;
            this.adj = adj;
            for (int i = 0; i < processorsReserved; i++) {
                parallelDistance.add(Integer.MAX_VALUE);
            }
        }

        public Node(int id, int... adj) {
            this.id = id;
            for (int j : adj) {
                this.adj.add(j);
            }
            for (int i = 0; i < processorsReserved; i++) {
                parallelDistance.add(Integer.MAX_VALUE);
            }
        }

        @Override
        public String toString() {
            return "\nNode{" +
                    "id=" + id +
                    ", serialDistance=" + serialDistance +
                    ", parallelDistance=" + parallelDistance +
                    ", adj=" + adj +
                    "}";
        }
    }
}

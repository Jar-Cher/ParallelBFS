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
        return nodes.size();
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
        int rank = MPI.COMM_WORLD.Rank();
        int processorsAmount = MPI.COMM_WORLD.Size();
        //System.out.println("From <"+rank+">: <"+size+">");
        int level = 0;
        int[] MPIdistances = IntStream.range(0, this.size()).map(it -> Integer.MAX_VALUE).toArray();
        int[] nextFrontier = new int[this.size()];
        nextFrontier[startingQueue.getFirst()] = 1;
        int[] visited = new int[this.size()];
        int[] localNodes = new int[this.size()];
        int[] adjMatrix = new int[this.size() * this.size()];
        //if (rank == 0) {
            for (int i = 0; i < this.size(); i++) {
                localNodes[i] = i;
                for (int j = 0; j < nodes.get(i).adj.size(); j++) {
                    adjMatrix[i * this.size() + nodes.get(i).adj.get(j)] = 1;
                }
            }
        //}
        MPI.COMM_WORLD.Barrier();
        //MPI.COMM_WORLD.Bcast(adjMatrix, 0, this.size() * this.size(), MPI.INT, 0);
        //MPI.COMM_WORLD.Bcast(localNodes, 0, this.size(), MPI.INT, 0);
        //System.out.println("Processor #<"+rank+"> adjMatrix is <"+ Arrays.toString(adjMatrix) +">");
        //System.out.println("Processor #<"+rank+"> localNodes is <"+ Arrays.toString(localNodes) +">");
        while(Arrays.stream(nextFrontier).anyMatch(it -> it == 1)) {
            int chunk = this.size() / processorsAmount + this.size() % processorsAmount;
            int[] currentFrontier = new int[chunk];
            //Arrays.stream(nextFrontier).filter(it -> ((it >= rank * chunk) && (it < (rank + 1) * chunk))).toArray();
            if ((rank + 1) * chunk - rank * chunk >= 0)
                System.arraycopy(nextFrontier, rank * chunk, currentFrontier, 0, Integer.min(chunk, this.size() - rank * chunk));
            nextFrontier = new int[this.size()];
            //System.out.println("Process <"+rank+"> have frontier of <"+ Arrays.toString(currentFrontier) +">");

            for (int i = 0; i < currentFrontier.length; i ++) {
                if (currentFrontier[i] == 1) {
                    int node = localNodes[i + rank * chunk];
                    //System.out.println("Processor <" + rank + "> now processes node #<" + node + ">");

                    if (visited[node] == 0) {
                        MPIdistances[node] = level;

                        for (int j = 0; j < this.size(); j ++) {
                            //System.out.println("Processor #<" + rank + "> works out adj (" + adjMatrix[node * this.size() + j] + ") of node #<" + node + ">");

                            if (adjMatrix[node * this.size() + j] == 1) {
                                //System.out.println("Processor #<" + rank + "> confirmed adj (" + adjMatrix[node * this.size() + j] + ") of node #<" + node + ">");
                                nextFrontier[j] = 1;
                                //System.out.println("Processor #<" + rank + "> confirmed new next frontier would be " + Arrays.toString(nextFrontier) + ">");
                            }
                        }
                        visited[node] = 1;
                    }
                }
            }

            int[] displs = IntStream.range(0, processorsAmount).map(it -> it * this.size()).toArray();//new int[processorsAmount];

//            MPI.COMM_WORLD.Barrier();
//            // MPI gather local frontiers sizes
//            int[] localFrontierSize = new int[1];
//            localFrontierSize[0] = nextFrontier.length;
            int[] nextFrontierSizes = IntStream.range(0, this.size()).map(it -> this.size()).toArray();
//            MPI.COMM_WORLD.Allgather(localFrontierSize, 0, 1, MPI.INT,
//                    nextFrontierSizes, 0, 1, MPI.INT);
//            System.out.println("Processor #"+rank+"> confirms sizes of new frontiers <"+ Arrays.toString(nextFrontierSizes) +">");

            //int[] = new int[processorsAmount];
            //MPI.COMM_WORLD.Barrier();
            // MPI gather next frontier
            //int[] localNext = nextFrontier.stream.mapToInt(it -> it).toArray();
            int[] nextFrontierArray = new int[processorsAmount * this.size()];
            MPI.COMM_WORLD.Allgatherv(nextFrontier, 0, nextFrontier.length, MPI.INT,
                    nextFrontierArray, 0, nextFrontierSizes, displs, MPI.INT);
            //System.out.println("Processor #<"+rank+"> says full new frontiers <"+ Arrays.toString(nextFrontierArray) +">");
            for (int i = 0; i < processorsAmount; i ++) {
                for (int j = 0; j < this.size(); j ++) {
                    nextFrontier[j] = Integer.max(nextFrontier[j], nextFrontierArray[i * this.size() + j]);
                }
            }
            //System.out.println("Processor #<"+rank+"> folds new frontier as <"+ Arrays.toString(nextFrontier) +">");


//            MPI.COMM_WORLD.Barrier();
//            // MPI gather local visited sizes
//            int[] localVisitedSize = new int[1];
//            localVisitedSize[0] = visited.size();
             int[] nextVisitedSizes = IntStream.range(0, this.size()).map(it -> this.size()).toArray();//new int[processorsAmount];
//            MPI.COMM_WORLD.Allgather(localVisitedSize, 0, 1, MPI.INT,
//                    nextVisitedSizes, 0, 1, MPI.INT);
//            System.out.println("Processor <"+rank+"> reports visited <"+ Arrays.toString(nextVisitedSizes) +"> nodes");

            //MPI.COMM_WORLD.Barrier();
            // MPI gather visited
            //int[] localVisited = visited.stream().mapToInt(it -> it).toArray();
            int[] visitedArray = new int[processorsAmount * this.size()];
            //System.out.println("Processor <"+rank+"> reports visited: <"+ Arrays.toString(localVisited) +"> nodes");
            //System.out.println("Processor <"+rank+"> reports visited following <"+ Arrays.toString(visitedArray) +"> nodes");
            MPI.COMM_WORLD.Allgatherv(visited, 0, visited.length, MPI.INT,
                    visitedArray, 0, nextVisitedSizes, displs, MPI.INT);
            //System.out.println("Processor <"+rank+"> reports visited following <"+ Arrays.toString(visitedArray) +"> nodes");
            for (int i = 0; i < processorsAmount; i ++) {
                for (int j = 0; j < this.size(); j ++) {
                    visited[j] = Integer.max(visited[j], visitedArray[i * this.size() + j]);
                }
            }
            //System.out.println("Processor <"+rank+"> folds visited as <"+ Arrays.toString(visited) +">");

            //MPI.COMM_WORLD.Barrier();
            int[] distancesArray = new int[processorsAmount * this.size()];
            MPI.COMM_WORLD.Allgatherv(MPIdistances, 0, MPIdistances.length, MPI.INT,
                    distancesArray, 0, nextVisitedSizes, displs, MPI.INT);
            for (int i = 0; i < processorsAmount; i ++) {
                for (int j = 0; j < this.size(); j ++) {
                    MPIdistances[j] = Integer.min(MPIdistances[j], distancesArray[i * this.size() + j]);
                }
            }
            //System.out.println("<"+rank+">: overall distances are <"+ Arrays.toString(MPIdistances) +">");

            level++;
            //System.out.println("\nNext level: " + level);
            //MPI.COMM_WORLD.Barrier();
        }
        if (rank == 0) {
            for (int i = 0; i < this.size(); i++) {
                nodes.get(i).parallelDistance.set(0, MPIdistances[i]);
            }
        }
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

            //System.out.println(processorsAmount);
        }
        for (int i = 0; i < processorsAmount; i++) {
            threads.get(i).interrupt();

        }
        System.out.println("Done");
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
                    //synchronized (queue) {
                        while (visitedNodes.contains(queue.peek())) {
                            queue.poll();
                            if (queue.isEmpty()) {
                                isFree = true;
                            }
                        }
                        next = queue.poll();
                        //while (visitedNodes.contains(next)) {
                        //    next = queue.poll();
                        //}
                        if ((next == null) || (isFree)) {
                            isFree = true;
                            continue;
                        }

                        queue.addAll(nodes.get(next).adj
                                .stream().filter(it -> !visitedNodes.contains(it)).collect(Collectors.toSet()));
                    //}
                    visitedNodes.add(next); // !!!
                    synchronized (nodes) {
                        for (Integer i : nodes.get(next).adj) {
                            //System.out.println("Got over here!");
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
        private List<Integer> adj = new CopyOnWriteArrayList<>();

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

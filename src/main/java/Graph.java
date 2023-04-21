import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

public class Graph implements Iterable<Integer> {
    private Integer startNodeId = null;
    private int processorsReserved = 5;
    private final Deque<Integer> startingQueue = new ArrayDeque<>();

    public final CopyOnWriteArrayList<Node> nodes = new CopyOnWriteArrayList<>();

    private final ConcurrentLinkedDeque<Integer> queue = new ConcurrentLinkedDeque<Integer>();
    private final CopyOnWriteArraySet<Integer> visitedNodes = new CopyOnWriteArraySet<Integer>();

    public Graph() {
    }

    public Graph(int nodesAmount) {
        this(nodesAmount, 5);
    }

    public Graph(int nodesAmount, int processorsReserved) {
        Random random = new Random();
        this.processorsReserved = processorsReserved;
        for (int i = 0; i < nodesAmount; i++) {
            ArrayList<Integer> adjNodes = new ArrayList<>();
            for (int j = 0; j < random.nextInt(nodesAmount); j++) {
                adjNodes.add(random.nextInt(nodesAmount));
            }
            this.addNode(adjNodes);
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

    public void addNode(ArrayList<Integer> adj) {
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

    public void parallelBFS(int processorsAmount) {

        final ArrayList<SearcherThread> threads = new ArrayList<>();
        visitedNodes.clear();
        queue.clear();
        queue.addAll(startingQueue);
        for (int i = 0; i < processorsAmount; i++) {
            SearcherThread thread = new SearcherThread(processorsAmount - 1);
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

    private class Node {
        private final int id;
        private int serialDistance = Integer.MAX_VALUE;
        private final CopyOnWriteArrayList<Integer> parallelDistance = new CopyOnWriteArrayList<>();
        private ArrayList<Integer> adj = new ArrayList<>();

        public Node(int id, ArrayList<Integer> adj) {
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
            return "Node{" +
                    "id=" + id +
                    ", serialDistance=" + serialDistance +
                    ", parallelDistance=" + parallelDistance +
                    ", adj=" + adj +
                    '}';
        }
    }
}

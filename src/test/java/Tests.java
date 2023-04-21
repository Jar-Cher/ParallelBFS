import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class Tests {

    @Test
    public void emptyTest() {
        Graph graph = new Graph();
        assertEquals("", graph.serialBFS());
    }

    @Test
    public void singletonTest() {
        Graph graph = new Graph();
        graph.addNode(0);
        assertEquals("0 ", graph.serialBFS());
    }

    @Test
    public void twinsTest() {
        Graph graph = new Graph();
        graph.addNode(1);
        graph.addNode(0);
        assertEquals("0 1 ", graph.serialBFS());
    }

    @Test
    public void wikiTest() {
        Graph graph = new Graph();
        graph.addNode(1, 2);
        graph.addNode(5, 6);
        graph.addNode(3, 4);
        graph.addNode();
        graph.addNode();
        graph.addNode();
        graph.addNode(7);
        graph.addNode();
        assertEquals("0 1 2 5 6 3 4 7 ", graph.serialBFS());
    }

    @Test
    public void StartNodeIdChangeTest() {
        Graph graph = new Graph(5);
        graph.addNode();
        graph.addNode(5);
        graph.setStartNodeId(6);
        assertEquals("6 5 ", graph.serialBFS());
    }

    @Test
    public void gfgTest() {
        Graph graph = new Graph();
        graph.addNode(1);
        graph.addNode(2);
        graph.addNode(0, 3);
        graph.addNode(3);
        graph.setStartNodeId(2);
        assertEquals("2 0 3 1 ", graph.serialBFS());
    }
}

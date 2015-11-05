package graphstream;

import misc.Constants;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.view.View;
import org.graphstream.ui.view.Viewer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class GraphViewer {

    public static void main(String args[]) throws IOException {
        System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
        Graph graph = new MultiGraph("Relationships");
        graph.addAttribute("ui.quality");
        graph.addAttribute("ui.antialias");
        graph.addAttribute("ui.stylesheet", "edge {fill-color:red; }");
        graph.addAttribute("ui.stylesheet", "edge {text-alignment: along; }");
        graph.addAttribute("ui.stylesheet", "graph { fill-color: grey; }");
        Map<String, String> nodeIds = addNodesFromFile(graph, Constants.VERTICES_FILENAME());
        addEdgesFromFile(graph, Constants.EDGES_FILENAME(), nodeIds);
        graph.display(true);
    }


    public static Map<String, String> addNodesFromFile(Graph graph, String fileName) throws IOException {
        Map<String, String> nodesMap = new HashMap<>();
        Path file = Paths.get(fileName);
        Files.lines(file).forEach(line -> {
            String[] values = line.split(",");
            Node node = graph.addNode(values[1]);
            node.addAttribute("ui.label", values[1]);
            node.addAttribute("ui.style", "text-offset: -10, -15;");
//            node.addAttribute("ui.style", "shadow-mode: plain;");
            nodesMap.put(values[0], values[1]);
        });
        return nodesMap;
    }

    public static void addEdgesFromFile(Graph graph, String fileName, Map<String, String> nodeIds) throws IOException {
        Path file = Paths.get(fileName);
        Files.lines(file).forEach(line -> {
            String[] values = line.split(" ");
            Edge edge = graph.addEdge(
                    new StringBuilder(values[0]).append("-").append(values[1]).toString(),
                    nodeIds.get(values[0]),
                    nodeIds.get(values[1]),
                    true
            );
            edge.setAttribute("ui.label", values[2]);
//            edge.setAttribute("ui.style", "text-offset: -30;");
        });
    }

}

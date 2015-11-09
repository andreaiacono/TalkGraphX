package graphstream;

import misc.Constants;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.swingViewer.ViewPanel;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SimpleGraphViewer {

    private final MultiGraph graph;
    private ViewPanel view;
    private double viewPercent = 0.7;

    public SimpleGraphViewer(String verticesFilename, String edgesFilename) throws IOException {

        // creates the graph and its attributes
        System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
        graph = new MultiGraph("Relationships");
        graph.addAttribute("ui.quality");
        graph.addAttribute("ui.antialias");
        graph.addAttribute("ui.stylesheet", "url('file:./" + Constants.CSS_FILENAME() + "')");

        // adds nodes and edges to the graph
        addDataFromFile(graph, verticesFilename, edgesFilename);
    }

    public void run() {
        // starts the GUI with a custom mouse wheel listener for zooming in and out
        view = graph.display(true).getDefaultView();
        view.addMouseWheelListener(event -> zoom(event.getWheelRotation() < 0));
    }

    public void zoom(boolean zoomOut) {
        viewPercent += viewPercent * 0.1 * (zoomOut ? -1 : 1);
        view.getCamera().setViewPercent(viewPercent);
    }

    public void addDataFromFile(Graph graph, String verticesFilename, String edgesFilename) throws IOException {
        Map<String, String> nodesMap = new HashMap<>();
        Set<String> addedEdges = new HashSet<>();

        // loads the nodes
        Path file = Paths.get(verticesFilename);
        Files.lines(file)
            .filter( line -> line.charAt(0) != '#')
            .forEach(line -> {
                String[] values = line.split(" ");
                Node node = graph.addNode(values[1]);
                StringBuilder label = new StringBuilder(values[1]);
                if (values.length > 2) label.append(",").append(values[2]);
                label.append(" [").append(values[0]).append("]");
                node.addAttribute("ui.label", label.toString());
                nodesMap.put(values[0], values[1]);
            });

        // loads the edges
        file = Paths.get(edgesFilename);
        Files.lines(file)
            .filter( line -> line.charAt(0) != '#')
            .forEach(line -> {
                String[] values = line.split(" ");
                boolean hasLabel = values.length > 2;
                String id = new StringBuilder(values[0]).append("-").append(values[1]).toString();
                String reverseId = new StringBuilder(values[1]).append("-").append(values[0]).toString();

                Edge edge = graph.addEdge(
                        id,
                        nodesMap.get(values[0]),
                        nodesMap.get(values[1]),
                        true
                );

                // shows labels correctly for parallel edges
                String offset = addedEdges.contains(reverseId) ? "0,-50" : "0,50";
                edge.addAttribute("ui.style", "text-offset: " + offset + ";");
                if (hasLabel) {
                    edge.setAttribute("ui.style", "fill-color:" + (values[2].equals("likes") ? "#00CC00":"#CC0000") + ";");
                    edge.setAttribute("ui.label", values[2]);
                }

                addedEdges.add(id);
            });
    }

    public static void main(String args[]) throws IOException {
        new SimpleGraphViewer(Constants.USERS_VERTICES_FILENAME(), Constants.USERS_EDGES_FILENAME()).run();
    }
}

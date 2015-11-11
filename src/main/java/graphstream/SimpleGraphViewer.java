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
import java.util.*;

public class SimpleGraphViewer {

    private final MultiGraph graph;
    private ViewPanel view;
    private double viewPercent = 0.7;

    public SimpleGraphViewer(String verticesFilename, String edgesFilename, boolean isDirected) throws IOException {

        // creates the graph and its attributes
        System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
        graph = new MultiGraph("Relationships");
        graph.addAttribute("ui.quality");
        graph.addAttribute("ui.antialias");
        graph.addAttribute("ui.stylesheet", "url('file:./" + Constants.CSS_FILENAME() + "')");

        // adds nodes and edges to the graph
        addDataFromFile(graph, verticesFilename, edgesFilename, isDirected);
    }

    public SimpleGraphViewer(String verticesFilename, String edgesFilename) throws IOException {
        this(verticesFilename, edgesFilename, true);
    }

    public void run() {
        // starts the GUI with a custom mouse wheel listener for zooming in and out
        view = graph.display(true).getDefaultView();
        view.resizeFrame(800, 600);
        view.addMouseWheelListener(event -> zoom(event.getWheelRotation() < 0));
    }

    public void zoom(boolean zoomOut) {
        viewPercent += viewPercent * 0.1 * (zoomOut ? -1 : 1);
        view.getCamera().setViewPercent(viewPercent);
    }

    public void addDataFromFile(Graph graph, String verticesFilename, String edgesFilename, boolean isDirected) throws IOException {
        Map<String, String> nodesMap = new HashMap<>();
        Set<String> addedEdges = new HashSet<>();

        // loads the nodes
        Path file = Paths.get(verticesFilename);
        Files.lines(file)
            .filter( line -> line.charAt(0) != '#')
            .forEach(line -> {
                String[] values = line.split(" ");
                nodesMap.put(values[0], values[1]);

                Node node = graph.addNode(values[1]);
                StringBuilder label = new StringBuilder(values[1]);
                if (values.length > 2) label.append(",").append(values[2]);
                label.append("[").append(values[0]).append("]");
                node.addAttribute("ui.label", label.toString());
            });

        // loads the edges
        file = Paths.get(edgesFilename);
        Files.lines(file)
            .filter( line -> line.charAt(0) != '#')
            .forEach(line -> {
                String[] values = line.split(" ");
                boolean hasLabel = values.length > 2;

                StringBuilder id = new StringBuilder(values[0]).append("-").append(values[1]);
                int counter = 0;
                // for allowing multiple edges with the same source and same destination
                while (addedEdges.contains(id.toString())) {
                    if (counter > 0) id.delete(id.lastIndexOf("-")+1, id.length()-1);
                    id.append("-").append(counter ++);
                }
                // FIXME: has to be fixed for multiple edges with same source and same dest
                String reverseId = new StringBuilder(values[1]).append("-").append(values[0]).toString();
                Edge edge = graph.addEdge(
                        id.toString(),
                        nodesMap.get(values[0]),
                        nodesMap.get(values[1]),
                        isDirected
                );

                // set layout attributes according to graph type
                StringBuilder uiStyleAttribute = new StringBuilder();
                if (hasLabel) {
                    edge.setAttribute("ui.label", values[2]);
                    uiStyleAttribute
                            .append("fill-color:")
                            .append((values[2].equals("likes") ? "#00CC00":"#0000BB"))
                            .append(";");
                }

                if (isDirected) {
                    uiStyleAttribute
                            .append("text-offset: ")
                            .append(addedEdges.contains(reverseId) ? "0,-10;" : "0,10;");
                }
                else {
                    uiStyleAttribute
                            .append("text-offset: 0,-10;")
                            .append("fill-color: #0000BB;");
                }
                edge.setAttribute("ui.style", uiStyleAttribute.toString());

                addedEdges.add(id.toString());
            });
    }
}

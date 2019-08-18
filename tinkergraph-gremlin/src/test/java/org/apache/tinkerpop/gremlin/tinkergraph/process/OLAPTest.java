package org.apache.tinkerpop.gremlin.tinkergraph.process;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class OLAPTest {
    @Test
    public void pr() {
        TinkerGraph graph = TinkerFactory.createModern();
        TraversalSource ts = graph.traversal();

        Vertex v1 = null;

        List<Vertex> list = graph.traversal().V(1).bothE().otherV().toStream().collect(Collectors.toList());

        for (Vertex v : list) System.out.println(v.id());

        System.out.println(v1);
/*        try {
            ComputerResult result = graph.compute().program(PageRankVertexProgram.build().create()).submit().get();
            System.out.println("iterations: " + result.memory().getIteration());
            result.graph().traversal().V().toStream().forEach( v -> {
                System.out.println(v.id() + "  " + v.property(PageRankVertexProgram.PAGE_RANK).key() + "  " + v.property(PageRankVertexProgram.PAGE_RANK).value()
                + " Edge count " + v.property("gremlin.pageRankVertexProgram.edgeCount").value());
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }*/
    }
}

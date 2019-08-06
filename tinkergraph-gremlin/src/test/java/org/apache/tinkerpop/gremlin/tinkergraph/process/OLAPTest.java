package org.apache.tinkerpop.gremlin.tinkergraph.process;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class OLAPTest {
    @Test
    public void pr() {
        TinkerGraph graph = TinkerFactory.createModern();
        TraversalSource ts = graph.traversal();

        try {
            ComputerResult result = graph.compute().program(PageRankVertexProgram.build().create()).submit().get();
            result.graph().traversal().V().toStream().forEach( v -> {
                System.out.println(v.id() + "  " + v.property(PageRankVertexProgram.PAGE_RANK).key() + "  " + v.property(PageRankVertexProgram.PAGE_RANK).value());
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}

package org.rdkit.neo4j.procedures;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.PagingIterator;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.rdkit.neo4j.models.Constants;
import org.rdkit.neo4j.models.NodeFields;

public abstract class BaseProcedure {
  static final String fingerprintProperty = NodeFields.FingerprintEncoded.getValue();
  static final String fingerprintOnesProperty = NodeFields.FingerprintOnes.getValue();
  static final String canonicalSmilesProperty = NodeFields.CanonicalSmiles.getValue();
  static final String indexName = Constants.IndexName.getValue();

  static final int PAGE_SIZE = 10_000;
  static final String PAGE_SIZE_STRING = "10000";

  @Context
  public GraphDatabaseService db;

  @Context
  public Log log;

  /**
   * Method checks existence of nodeIndex
   * If it does not exist, fulltext query will not be executed (lucene does not contain the data)
   * @param labelNames to query on
   * @param indexName to look for
   */
  void checkIndexExistence(List<String> labelNames, String indexName) {
    Set<Label> labels = labelNames.stream().map(Label::label).collect(Collectors.toSet());

    try {
      IndexDefinition index = db.schema().getIndexByName(indexName);
      assert index.isNodeIndex();
      assert StreamSupport.stream(index.getLabels().spliterator(), false).allMatch(labels::contains);
    } catch (AssertionError e) {
      log.error("No `%s` node index found", indexName);
      throw e;
    }
  }

  void createFullTextIndex(final String indexName, final List<String> labelNames, final List<String> properties) {
    Map<String, Object> params = MapUtil.map(
        "index", indexName,
        "labels", labelNames,
        "property", properties
    );

    db.execute("CALL db.index.fulltext.createNodeIndex($index, $labels, $property, {analyzer: 'whitespace'} )", params);
  }

  /**
   * Method returns nodes with specified labels
   * @param labelNames list
   * @return stream of nodes
   */
  Stream<Node> getLabeledNodes(List<String> labelNames) {
    final String firstLabel = labelNames.get(0);
    final List<Label> labels = labelNames.stream().map(Label::label).collect(Collectors.toList());

    return db.findNodes(Label.label(firstLabel))
        .stream()
//        .parallel()
        .filter(node -> labels.stream().allMatch(node::hasLabel));
  }


  // todo: requires great explanation
  // todo: requires refactor (speed up)
  void executeBatches(final Stream<Node> nodes, final int batchSize, final int parallelism, Consumer<? super Node> nodeAction) throws InterruptedException {

    log.info("starting batch processing with batchsize of %d and a parallelism of %d", batchSize, parallelism);
    long now = System.currentTimeMillis();
    ExecutorService executorService = Executors.newWorkStealingPool(parallelism);

    Iterator<Node> nodeIterator = nodes.iterator();
    final PagingIterator<Node> pagingIterator = new PagingIterator<>(nodeIterator, batchSize);
    final AtomicInteger numberOfBatches = new AtomicInteger();
    while (pagingIterator.hasNext()) {
      Iterator<Node> page = pagingIterator.nextPage();

      final List<Node> materializedNodesForBatch = Iterators.asList(page);

      executorService.execute(() -> {
        try (Transaction tx = db.beginTx()) {
          int batchNumber = numberOfBatches.getAndIncrement();
          log.info("starting batch # %d", batchNumber);
//          materializedNodesForBatch.forEach(node -> {}); // for checking baseline
          materializedNodesForBatch.forEach(nodeAction);
          tx.success();
          log.info("done batch # %d", batchNumber);
        }
      });
    }

    executorService.shutdown();
    boolean hasTerminatedInTime = executorService.awaitTermination(10, TimeUnit.MINUTES);
    log.info("done with processing %d batches in %f secs, termination %s", numberOfBatches.get(), (System.currentTimeMillis()-now)/1000.0,hasTerminatedInTime);
  }

}

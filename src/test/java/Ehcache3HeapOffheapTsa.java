import io.rainfall.ObjectGenerator;
import io.rainfall.Runner;
import io.rainfall.Scenario;
import io.rainfall.SyntaxException;
import io.rainfall.configuration.ConcurrencyConfig;
import io.rainfall.ehcache.statistics.EhcacheResult;
import io.rainfall.ehcache3.Ehcache3Operations;
import io.rainfall.generator.ByteArrayGenerator;
import io.rainfall.generator.LongGenerator;
import io.rainfall.generator.sequence.Distribution;
import io.rainfall.statistics.StatisticsPeekHolder;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expirations;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;

import static io.rainfall.configuration.ReportingConfig.html;
import static io.rainfall.configuration.ReportingConfig.report;
import static io.rainfall.configuration.ReportingConfig.text;
import static io.rainfall.ehcache.statistics.EhcacheResult.GET;
import static io.rainfall.ehcache.statistics.EhcacheResult.MISS;
import static io.rainfall.ehcache.statistics.EhcacheResult.PUT;
import static io.rainfall.ehcache3.CacheConfig.cacheConfig;
import static io.rainfall.ehcache3.Ehcache3Operations.get;
import static io.rainfall.ehcache3.Ehcache3Operations.put;
import static io.rainfall.ehcache3.Ehcache3Operations.remove;
import static io.rainfall.execution.Executions.during;
import static io.rainfall.unit.TimeDivision.minutes;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;


/**
 * export MAVEN_OPTS="-Xms3g -Xmx6g -XX:MaxPermSize=512m -XX:MaxDirectMemorySize=200g -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:verbose-gc.log -server"
 * <p/>
 * mvn test -Dtest=Ehcache3HeapOffheapTsa -Dheap=250 -Doffheap=1 -Dtsa=1 -DtestLength=2 -DnbThreads=4 -DnbClients=1 -DreportDir=ehc3 -DclientId=0
 *
 * @author Aurelien Broszniowski
 */
public class Ehcache3HeapOffheapTsa {

  protected Logger logger = LoggerFactory.getLogger(getClass());

  @Test
  public void testHeapOffheapDisk() {
    int heap = Integer.parseInt(System.getProperty("heap"));
    int offheap = Integer.parseInt(System.getProperty("offheap"));
    int tsa = Integer.parseInt(System.getProperty("tsa"));

    long nbElementsHeap = MemoryUnit.MB.toBytes(heap) / MemoryUnit.KB.toBytes(1);
    long nbElements = MemoryUnit.GB.toBytes(tsa) / MemoryUnit.KB.toBytes(1);

    String tcHost = "localhost";
    CacheManagerBuilder<PersistentCacheManager> managerBuilder = newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://" + tcHost + ":9510/my-application"))
            .autoCreate()
        )
        .withCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, byte[].class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(nbElementsHeap, EntryUnit.ENTRIES)
                .offheap(offheap, MemoryUnit.GB)
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", tsa, MemoryUnit.GB)))
            .withExpiry(Expirations.noExpiration())
            .add(new ClusteredStoreConfiguration(Consistency.EVENTUAL))
            .build());

    String reportName = "tsa-" + heap;

    ObjectGenerator<Long> keyGenerator = new LongGenerator();
    ObjectGenerator<byte[]> valueGenerator = ByteArrayGenerator.fixedLength(1024);

    int nbThreads = Integer.parseInt(System.getProperty("nbThreads", "" + Runtime.getRuntime().availableProcessors()));
    ConcurrencyConfig concurrency = ConcurrencyConfig.concurrencyConfig().threads(nbThreads).timeout(1, DAYS);

    String reportDir = System.getProperty("reportDir", ".");

    PersistentCacheManager cacheManager = null;
    try {
      cacheManager = managerBuilder.build(true);

      Cache<Long, byte[]> cache = cacheManager.getCache("cache", Long.class, byte[].class);

      Integer testLength = Integer.parseInt(System.getProperty("testLength", "7"));

      logger.info("----------> Test phase");
      StatisticsPeekHolder finalStats = Runner.setUp(
          Scenario.scenario("Test phase")
              .exec(
                  get(Long.class, byte[].class).using(keyGenerator, valueGenerator)
                      .atRandom(Distribution.SLOW_GAUSSIAN, 0, nbElements, nbElements / 10)
                      .withWeight(0.70),
                  put(Long.class, byte[].class).using(keyGenerator, valueGenerator)
                      .atRandom(Distribution.SLOW_GAUSSIAN, 0, nbElements, nbElements / 10)
                      .withWeight(0.10),
                  remove(Long.class, byte[].class).using(keyGenerator, valueGenerator)
                      .atRandom(Distribution.SLOW_GAUSSIAN, 0, nbElements, nbElements / 10)
                      .withWeight(0.10),
                  Ehcache3Operations.replaceForKeyAndValue(Long.class, byte[].class).using(keyGenerator, valueGenerator)
                      .atRandom(Distribution.SLOW_GAUSSIAN, 0, nbElements, nbElements / 10)
                      .withWeight(0.10)
              ))
          .warmup(during(2, minutes))
          .executed(during(testLength, minutes))
          .config(concurrency)
          .config(report(EhcacheResult.class)
              .log(text(), html(reportDir + File.separatorChar + "test-" + reportName)))
          .config(cacheConfig(Long.class, byte[].class)
              .cache("cache", cache))
          .start();
      logger.info("----------> Done");
    } catch (SyntaxException e) {
      e.printStackTrace();
    } finally {
      if (cacheManager != null) {
        cacheManager.close();
      }
    }
  }
}

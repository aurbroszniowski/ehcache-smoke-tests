import io.rainfall.ObjectGenerator;
import io.rainfall.SyntaxException;
import io.rainfall.generator.ByteArrayGenerator;
import io.rainfall.generator.LongGenerator;
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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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
    final long nbElements = MemoryUnit.GB.toBytes(tsa) / MemoryUnit.KB.toBytes(1);

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

    final ObjectGenerator<Long> keyGenerator = new LongGenerator();
    final ObjectGenerator<byte[]> valueGenerator = ByteArrayGenerator.fixedLength(1024);

    int nbThreads = Integer.parseInt(System.getProperty("nbThreads", "" + Runtime.getRuntime().availableProcessors()));
    String reportDir = System.getProperty("reportDir", ".");

    PersistentCacheManager cacheManager = null;
    try {
      cacheManager = managerBuilder.build(true);

      final Cache<Long, byte[]> cache = cacheManager.getCache("cache", Long.class, byte[].class);

      Integer testLength = Integer.parseInt(System.getProperty("testLength", "7"));

      logger.info("----------> Test phase");
      List<Thread> threads = new ArrayList<Thread>();
      for (int j = 0; j < nbThreads; j++) {
        threads.add(new Thread(new Runnable() {
          public void run() {
            for (long i = 0; i < nbElements; i++) {
              cache.put(keyGenerator.generate(i), valueGenerator.generate(i));
              cache.get(keyGenerator.generate(i));
              cache.remove(keyGenerator.generate(i));
              cache.putIfAbsent(keyGenerator.generate(i), valueGenerator.generate(i));
              cache.replace(keyGenerator.generate(i), valueGenerator.generate(i));
            }
          }
        }));
      }

      for (Thread thread : threads) {
        thread.start();
        try {
          thread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      logger.info("----------> Done");
    } finally {
      if (cacheManager != null) {
        cacheManager.close();
      }
    }
  }
}

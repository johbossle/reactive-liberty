package product.service.application;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.HystrixThreadPoolProperties;
import com.netflix.hystrix.strategy.concurrency.HystrixConcurrencyStrategy;
import com.netflix.hystrix.strategy.properties.HystrixProperty;

/**
 * A WAS specific implementation of the HystrixConcurrencyStrategy that uses a ManagedThreadFactory under
 * the JNDI name "concurrent/hystrix/threadFactory"
 * 
 * @see http://netflix.github.io/Hystrix/javadoc/index.html?com/netflix/hystrix/strategy/concurrency/HystrixConcurrencyStrategy.html
 * @author skliche
 *
 */
public class HystrixConcurrencyStrategyLiberty extends HystrixConcurrencyStrategy {
	private static final String THREAD_FACTORY_JNDI = "concurrent/hystrix/threadFactory";
	private static final Logger logger = LoggerFactory.getLogger(HystrixConcurrencyStrategy.class);
	private InitialContext ic;
	
	public HystrixConcurrencyStrategyLiberty() {
		try {
			this.ic =  new InitialContext();
		} catch (NamingException e) {
        	throw new IllegalStateException(e);
        } 
	}
	
	@Override
	public ThreadPoolExecutor getThreadPool(HystrixThreadPoolKey threadPoolKey, HystrixProperty<Integer> corePoolSize,
			HystrixProperty<Integer> maximumPoolSize, HystrixProperty<Integer> keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue) {
	  ThreadFactory threadFactory =  lookupThreadFactory(threadPoolKey);
	  
	  final int dynamicCoreSize = corePoolSize.get();
    final int dynamicMaximumSize = maximumPoolSize.get();

    if (dynamicCoreSize > dynamicMaximumSize) {
      logger.error("Hystrix ThreadPool configuration at startup for : {} is trying to set coreSize = {} and " +
          "maximumSize = {}.  Maximum size will be set to {}, the coreSize value, since it must be equal to or " +
          "greater than the coreSize value", threadPoolKey.name(), dynamicCoreSize, dynamicMaximumSize, dynamicCoreSize);
        return new ThreadPoolExecutor(dynamicCoreSize, dynamicCoreSize, keepAliveTime.get(), unit, workQueue, threadFactory);
    } 
    return new ThreadPoolExecutor(dynamicCoreSize, dynamicMaximumSize, keepAliveTime.get(), unit, workQueue, threadFactory);
  }
	
	private ThreadFactory lookupThreadFactory(HystrixThreadPoolKey threadPoolKey){
	  ThreadFactory threadFactory;
	  try {
      threadFactory = (ThreadFactory) ic.lookup(THREAD_FACTORY_JNDI);
    } catch (NamingException e) {
      throw new IllegalStateException(e);
    } 
	  return threadFactory;
	}
	
	@Override
	public ThreadPoolExecutor getThreadPool(final HystrixThreadPoolKey threadPoolKey, HystrixThreadPoolProperties threadPoolProperties) {
    final ThreadFactory threadFactory =  lookupThreadFactory(threadPoolKey);

    final boolean allowMaximumSizeToDivergeFromCoreSize = threadPoolProperties.getAllowMaximumSizeToDivergeFromCoreSize().get();
    final int dynamicCoreSize = threadPoolProperties.coreSize().get();
    final int keepAliveTime = threadPoolProperties.keepAliveTimeMinutes().get();
    final int maxQueueSize = threadPoolProperties.maxQueueSize().get();
    final BlockingQueue<Runnable> workQueue = getBlockingQueue(maxQueueSize);

    if (allowMaximumSizeToDivergeFromCoreSize) {
        final int dynamicMaximumSize = threadPoolProperties.maximumSize().get();
        if (dynamicCoreSize > dynamicMaximumSize) {
            logger.error("Hystrix ThreadPool configuration at startup for : {} is trying to set coreSize = {} and " +
                "maximumSize = {}.  Maximum size will be set to {}, the coreSize value, since it must be equal to or " +
                "greater than the coreSize value", threadPoolKey.name(), dynamicCoreSize, dynamicMaximumSize, dynamicCoreSize);
            return new ThreadPoolExecutor(dynamicCoreSize, dynamicCoreSize, keepAliveTime, TimeUnit.MINUTES, workQueue, threadFactory);
        } else {
            return new ThreadPoolExecutor(dynamicCoreSize, dynamicMaximumSize, keepAliveTime, TimeUnit.MINUTES, workQueue, threadFactory);
        }
    } else {
        return new ThreadPoolExecutor(dynamicCoreSize, dynamicCoreSize, keepAliveTime, TimeUnit.MINUTES, workQueue, threadFactory);
    }
	}
}

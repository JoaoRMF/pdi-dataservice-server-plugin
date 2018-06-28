package org.pentaho.di.trans.dataservice.streaming;

import io.reactivex.functions.Consumer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.streaming.execution.StreamExecutionListener;

import java.util.List;

public class GeneratedTransformationCacheUtil {

  /**
   * Generates the cache key for a given query with a specific size and rate.
   *
   * @param query The query.
   * @param mode The query window mode.
   * @param size The query window size.
   * @param every The query window rate.
   * @param maxRows The query window max rows.
   * @param maxTime The query window max time.
   * @return The cache key for the query.
   */
  public static String getCacheKey( String query, IDataServiceClientService.StreamingMode windowMode, long windowSize, long windowEvery,
                                    int windowMaxRowLimit, long windowMaxTimeLimit, long windowLimit ) {

    boolean timeBased = IDataServiceClientService.StreamingMode.TIME_BASED.equals( windowMode );

    int maxRows = getMaxRows( windowMaxRowLimit, windowLimit, windowMode );

    long maxTime = getMaxTime( windowMaxTimeLimit, windowLimit, windowMode );

    windowSize = windowSize <= 0 ? 0
      : ( timeBased ? Math.min( windowSize, maxTime ) : Math.min( windowSize, maxRows ) );
    windowEvery = windowEvery <= 0 ? 0
      : ( timeBased ? Math.min( windowEvery, maxTime ) : Math.min( windowEvery, maxRows ) );

    if ( windowSize == 0 ) {
      return null;
    }

    return query.concat( windowMode.toString() ).concat( "-" ).concat( String.valueOf( windowSize ) )
      .concat( "-" ).concat( String.valueOf( windowEvery ) )
      .concat( "-" ).concat( String.valueOf( maxRows ) )
      .concat( "-" ).concat( String.valueOf( maxTime ) );
  }

  public static long getMaxTime( long windowMaxTimeLimit, long windowLimit, IDataServiceClientService.StreamingMode windowMode ) {
    boolean rowBased = IDataServiceClientService.StreamingMode.ROW_BASED.equals( windowMode );
    return windowLimit > 0 && rowBased ? Math.min( windowLimit, windowMaxTimeLimit )
        : windowMaxTimeLimit;
  }

  public static int getMaxRows( int windowMaxRowLimit, long windowLimit, IDataServiceClientService.StreamingMode windowMode ) {
    boolean timeBased = IDataServiceClientService.StreamingMode.TIME_BASED.equals( windowMode );
    return windowLimit > 0 && timeBased ? (int) Math.min( windowLimit, windowMaxRowLimit )
        : windowMaxRowLimit;
  }
}

/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.streaming.execution;

import com.google.common.base.Throwables;
import io.reactivex.Observer;
import io.reactivex.subjects.ReplaySubject;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.utils.DataServiceConstants;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class to represent the Streaming execution of boundary Generated Transformation (SQL query over dataservices)
 */
public class StreamingGeneratedTransExecution implements Runnable {
  private final StreamingServiceTransExecutor serviceExecutor;
  private final Trans genTrans;
  private final Observer<RowMetaAndData> rowConsumer;
  private final String injectorStepName;
  private final String resultStepName;
  private ReplaySubject<RowMetaAndData> genTransCachePublishSubject = ReplaySubject.create();
  private final List<Observer<RowMetaAndData>> consumersList = new ArrayList<>( );
  private final String query;
  private final AtomicBoolean isRunning = new AtomicBoolean( false );

  private StreamExecutionListener stream;

  private IDataServiceClientService.StreamingMode windowMode;
  private long windowSize;
  private long windowEvery;
  private long windowLimit;

  /**
   * Constructor.
   *
   * @param serviceExecutor The {@link StreamingServiceTransExecutor} service transformation executor object.
   * @param genTrans The {@link org.pentaho.di.trans.Trans} generated transformation.
   * @param rowConsumer The consumer to be registered in the publish subject strream
   * @param injectorStepName The name of the step in the generated transformation where rows are injected.
   * @param resultStepName The name of the step in the generated transformation where the results are retreived.
   * @param query The query to be executed.
   * @param windowMode The streaming window mode.
   * @param windowSize The query window size. Number of rows for boundary ROW_BASED streamingType and milliseconds for boundary
   *                 TIME_BASED streamingType.
   * @param windowEvery The query window rate. Number of rows for boundary ROW_BASED streamingType and milliseconds for boundary
   *                 TIME_BASED streamingType.
   * @param windowLimit The query max window size. Number of rows for boundary TIME_BASED streamingType and milliseconds for boundary
   *                 ROW_BASED streamingType.
   */
  public StreamingGeneratedTransExecution( final StreamingServiceTransExecutor serviceExecutor, final Trans genTrans,
                                           final Observer<RowMetaAndData> rowConsumer, final String injectorStepName,
                                           final String resultStepName, final String query,
                                           final IDataServiceClientService.StreamingMode windowMode,
                                           long windowSize, long windowEvery, long windowLimit ) {
    this.serviceExecutor = serviceExecutor;
    this.genTrans = genTrans;
    this.rowConsumer = rowConsumer;
    this.injectorStepName = injectorStepName;
    this.resultStepName = resultStepName;
    this.query = query;
    this.windowMode = windowMode;
    this.windowSize = windowSize;
    this.windowEvery = windowEvery;
    this.windowLimit = windowLimit;

    this.addNewRowConsumer( rowConsumer );
  }

  /**
   * Spans boundary thread to execute the transformation.
   */
  @Override public void run() {
    // This is where we will inject the rows from the service transformation step
    if ( this.stream == null ) {
      this.stream = serviceExecutor.getBuffer( query,
        rowMetaAndDataList -> this.runGenTrans( rowMetaAndDataList ),
        windowMode, windowSize, windowEvery, windowLimit );
    }
    try {
      if ( stream == null ) {
        this.runGenTrans( Collections.emptyList() );
      }
    } catch ( KettleStepException e ) {
      throw Throwables.propagate( e );
    }
  }

  /**
   * Runs the generated transformation with the rows given by param.
   *
   * @param rowIterator The {@link List} input rows.
   * @throws KettleStepException
   */
  private void runGenTrans( final List<RowMetaAndData> rowIterator ) throws KettleStepException {
    if  ( isRunning.compareAndSet( false, true ) ) {
      try {
        LogChannelInterface log = genTrans.getLogChannel();
        RowProducer rowProducer;
        StepInterface resultStep;
        this.genTransCachePublishSubject = ReplaySubject.create();
        this.consumersList.stream().forEach( rowMetaAndDataObserver -> this.genTransCachePublishSubject.subscribe( rowMetaAndDataObserver ) );

        genTrans.cleanup();

        genTrans.prepareExecution( null );
        rowProducer = genTrans.addRowProducer( injectorStepName, 0 );

        genTrans.startThreads();

        resultStep = genTrans.findRunThread( resultStepName );

        resultStep.cleanup();
        resultStep.addRowListener( new RowListener() {
            @Override public void rowReadEvent( RowMetaInterface rowMetaInterface, Object[] objects ) throws KettleStepException {
              StreamingGeneratedTransExecution.this.genTransCachePublishSubject.onNext( new RowMetaAndData( rowMetaInterface, objects ) );
            }

            @Override public void rowWrittenEvent( RowMetaInterface rowMetaInterface, Object[] objects ) throws KettleStepException {
            }

            @Override public void errorRowWrittenEvent( RowMetaInterface rowMetaInterface, Object[] objects ) throws KettleStepException {
            }
        } );

        for ( RowMetaAndData injectRows : rowIterator ) {
          while ( !rowProducer.putRowWait( injectRows.getRowMeta(), injectRows.getData(), 1, TimeUnit.SECONDS )
            && genTrans.isRunning() ) {
            // Row queue was full, try again
            log.logRowlevel( DataServiceConstants.ROW_BUFFER_IS_FULL_TRYING_AGAIN );
          }
        }
        rowProducer.finished();
        genTrans.waitUntilFinished();
        genTrans.stopAll();

        this.consumersList.stream().forEach( rowMetaAndDataObserver -> rowMetaAndDataObserver.onComplete() );
        this.genTransCachePublishSubject.onComplete();

        log.logDetailed( DataServiceConstants.STREAMING_GENERATED_TRANSFORMATION_STOPPED );
      } catch ( KettleException e ) {
        throw new KettleStepException( e );
      } finally {
        isRunning.set( false );
        genTrans.setRunning( false );
        genTrans.setStopped( true );
      }
    }
  }

  public void addNewRowConsumer( final Observer<RowMetaAndData> consumer ) {
    this.consumersList.add( consumer );
    this.genTransCachePublishSubject.subscribe( consumer );
  }

  public void clearRowConsumers( ) {
    this.consumersList.stream().forEach( rowMetaAndDataObserver -> rowMetaAndDataObserver.onComplete() );
    this.consumersList.clear();
    this.genTransCachePublishSubject.onComplete();
    this.genTransCachePublishSubject = ReplaySubject.create();
  }

  public void clearRowConsumer( final Observer<RowMetaAndData> consumer ) {
    this.consumersList.remove( consumer );
    consumer.onComplete();
  }
}

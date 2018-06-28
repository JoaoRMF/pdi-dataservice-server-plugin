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

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.subjects.PublishSubject;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class to represents a listener for a service transformation stream.
 */
public class StreamExecutionListener {
  private IDataServiceClientService.StreamingMode windowMode;
  private long windowSize;
  private long windowEvery;
  private int maxRows;
  private long maxTime;
  private Disposable starterSubject;
  private Disposable subject;
  private Disposable fallbackSubject;
  private Observable<Observable<RowMetaAndData>> buffer;
  private Observable<Observable<RowMetaAndData>> fallbackBuffer;
  private List<RowMetaAndData> cachedWindow = Collections.synchronizedList( new ArrayList<RowMetaAndData>() );
  private PublishSubject<List<RowMetaAndData>> outputBufferPublisher;
  private final AtomicBoolean isProcessing = new AtomicBoolean( false );
  private final AtomicBoolean hasWindow = new AtomicBoolean( false );

  /**
   * Constructor.
   * Subscribes a listener to the given window buffer.
   *
   * @param stream The {@link io.reactivex.subjects.PublishSubject} data stream.
   * @param windowMode The streaming window mode.
   * @param windowSize The window size. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                 TIME_BASED streamingType.
   * @param windowEvery The window rate. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                 TIME_BASED streamingType.
   * @param maxRows The max rows window size.
   * @param maxTime The max time window size.
   */
  public StreamExecutionListener( final PublishSubject<RowMetaAndData> stream, Consumer<List<RowMetaAndData>> windowConsumer,
                                  final IDataServiceClientService.StreamingMode windowMode, final long windowSize,
                                  final long windowEvery, final int maxRows, final long maxTime ) {
    this.windowMode = windowMode;
    this.windowSize = windowSize;
    this.windowEvery = windowEvery;
    this.maxRows = maxRows;
    this.maxTime = maxTime;

    init( stream, windowConsumer );
  }

  public StreamExecutionListener( final PublishSubject<RowMetaAndData> stream,
                                  final IDataServiceClientService.StreamingMode windowMode, final long windowSize,
                                  final long windowEvery, final int maxRows, final long maxTime ) {
    this( stream, null, windowMode, windowSize, windowEvery, maxRows, maxTime );
  }

  /**
   * Inits the listener streaming buffers.
   * @param stream The {@link io.reactivex.subjects.PublishSubject} data stream.
   */
  private void init( PublishSubject<RowMetaAndData> stream, Consumer<List<RowMetaAndData>> windowConsumer ) {
    boolean rowBased = IDataServiceClientService.StreamingMode.ROW_BASED.equals( windowMode );
    boolean timeBased = IDataServiceClientService.StreamingMode.TIME_BASED.equals( windowMode );

    if ( windowEvery > 0 ) {
      if ( timeBased ) {
        this.buffer = stream.window( windowSize, windowEvery, TimeUnit.MILLISECONDS );
        this.fallbackBuffer = stream.window( maxRows );
      } else if ( rowBased ) {
        this.buffer = stream.window( (int) windowSize, (int) windowEvery );
        this.fallbackBuffer = stream.window( maxTime, TimeUnit.MILLISECONDS );
      }
    } else if ( timeBased ) {
      this.buffer = stream.window( windowSize, TimeUnit.MILLISECONDS );
      this.fallbackBuffer = stream.window( maxRows );
    } else {
      this.buffer = stream.window( (int) windowSize );
      this.fallbackBuffer = stream.window( maxTime, TimeUnit.MILLISECONDS );
    }

    this.outputBufferPublisher = PublishSubject.create();
    this.outputBufferPublisher.subscribe( windowConsumer );


    this.subject = this.buffer.subscribe( observableWindow -> processBufferWindow( observableWindow ) );
    this.fallbackSubject = this.fallbackBuffer.subscribe( observableWindow -> processFallbackWindow( observableWindow ) );

    if(timeBased) {
      this.starterSubject = stream.buffer( windowEvery, TimeUnit.MILLISECONDS ).cache().subscribe( items -> {
        if ( !hasWindow.get() ) {
          this.cachedWindow.addAll( items );
          outputBufferPublisher.onNext( this.cachedWindow );
        }
      } );
    } else {
      this.starterSubject = stream.buffer( (int) windowEvery ).subscribe( items -> {
        if ( !hasWindow.get() ) {
          this.cachedWindow.addAll( items );
          outputBufferPublisher.onNext( this.cachedWindow );
        }
      } );
    }
  }

  /**
   * Getter for the cached window data.
   *
   * @return The {@link List} cached window data.
   */
  public List<RowMetaAndData> getCachedWindow() {
    return this.cachedWindow;
  }

  /**
   * Un-subscribes the streaming buffers.
   */
  public void unSubscribe() {
    unSubscribeStarter();
    unSubscribeBuffer();
    unSubscribeFallbackBuffer();
  }

  /**
   * Un-subscribes the starter streaming buffer.
   */
  @VisibleForTesting
  protected void unSubscribeStarter() {
    if ( this.starterSubject != null ) {
      this.starterSubject.dispose();
      this.starterSubject = null;
    }
    this.cachedWindow.clear();
  }

  /**
   * Un-subscribes the streaming buffer.
   */
  @VisibleForTesting
  protected void unSubscribeBuffer() {
    if ( this.subject != null ) {
      this.subject.dispose();
      this.subject = null;
    }
  }

  /**
   * Un-subscribes the streaming fallback buffer.
   */
  @VisibleForTesting
  protected void unSubscribeFallbackBuffer() {
    if ( this.fallbackSubject != null ) {
      this.fallbackSubject.dispose();
      this.fallbackSubject = null;
    }
  }

  /**
   * Processes a buffer data window.
   *
   * @return The {@link io.reactivex.Observable} window buffer to process.
   */
  private void processBufferWindow( Observable<RowMetaAndData> list ) throws ExecutionException, InterruptedException {
    List<RowMetaAndData> bufferWindow = Collections.synchronizedList( new ArrayList<RowMetaAndData>() );
    list.doOnComplete( () -> {
      if ( isProcessing.compareAndSet( false, true ) ) {
        hasWindow.set( true );
        unSubscribeStarter();

        if ( this.fallbackSubject != null ) {
          this.fallbackSubject.dispose();
          this.fallbackSubject = this.fallbackBuffer.subscribe( bufferList -> processFallbackWindow( bufferList ) );
        }

        outputBufferPublisher.onNext( bufferWindow );
        isProcessing.set( false );
      }
    } ).subscribe( rowMetaAndData -> {
      bufferWindow.add( rowMetaAndData );
    } );
  }

  /**
   * Processes a fallback buffer data window.
   *
   * @return The {@link io.reactivex.Observable} fallback window buffer to process.
   */
  private void processFallbackWindow( Observable<RowMetaAndData> list ) throws ExecutionException, InterruptedException {
    List<RowMetaAndData> bufferWindow = Collections.synchronizedList( new ArrayList<RowMetaAndData>() );
    list.doOnComplete( () -> {
      if ( isProcessing.compareAndSet( false, true ) ) {
        hasWindow.set( true );
        unSubscribeStarter();

        if ( this.subject != null ) {
          this.subject.dispose();
          this.subject = this.buffer.subscribe( bufferList -> processBufferWindow( bufferList ) );
        }
        outputBufferPublisher.onNext( bufferWindow );
        isProcessing.set( false );
      }
    } ).subscribe( rowMetaAndData -> {
      bufferWindow.add( rowMetaAndData );
    } );
  }

  /**
   * Sets the cache window data with the given list.
   *
   * @param list - The {@link List} to set to cache.
   */
  private void setCacheWindow( List<RowMetaAndData> list ) {
    this.cachedWindow.clear();
    this.cachedWindow.addAll( list );
  }
}

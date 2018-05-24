package org.pentaho.di.trans.dataservice.api;

import org.pentaho.det.api.data.access.query.IQuery;

import javax.ws.rs.core.StreamingOutput;

public interface IDataservicesPushEndpoint {

  StreamingOutput queryDataservicePush( String dataserviceName, String windowMode, long windowSize, long windowEvery, long windowMax, IQuery query );

}

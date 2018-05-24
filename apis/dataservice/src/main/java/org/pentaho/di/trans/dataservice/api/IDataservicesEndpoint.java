package org.pentaho.di.trans.dataservice.api;

import org.pentaho.det.api.data.access.query.IQuery;

import javax.ws.rs.core.Response;

public interface IDataservicesEndpoint {

  Response queryDataservice( String dataserviceName, IQuery query );

}

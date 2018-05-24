package org.pentaho.di.trans.dataservice.endpoint;

import org.pentaho.det.api.data.access.query.IQuery;
import org.pentaho.di.trans.dataservice.api.IDataservicesEndpoint;
import org.pentaho.di.trans.dataservice.api.IDataservicesPushEndpoint;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;

@Path( "/dataservices" )
public class DataServicePushEndpoint implements IDataservicesPushEndpoint {

  public DataServicePushEndpoint() {

  }

  @POST
  @Path( "/queryDataservicePush/{dataserviceName}" )
  @Consumes( MediaType.APPLICATION_JSON )
  @Produces( MediaType.APPLICATION_JSON )
  @Override
  public StreamingOutput queryDataservicePush( @PathParam( "dataserviceName" ) String dataserviceName,
                                               String windowMode,
                                               long windowSize,
                                               long windowEvery,
                                               long windowMax,
                                               IQuery query ) {
    return new StreamingOutput() {
      public void write(final OutputStream out) throws IOException, WebApplicationException {
        //TODO
      }
    };
  }
}

package org.pentaho.di.trans.dataservice.endpoint;

import org.pentaho.det.api.data.access.query.IQuery;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.api.IDataservicesEndpoint;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.clients.Query;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolverDelegate;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

@Path( "/dataservices" )
public class DataServiceEndpoint implements IDataservicesEndpoint {

  private DataServiceResolverDelegate resolver;

  public DataServiceEndpoint( DataServiceResolverDelegate resolver) {
    this.resolver = resolver;
  }

  @POST
  @Path( "/queryDataservice/{dataserviceName}" )
  @Consumes( MediaType.APPLICATION_JSON )
  @Produces( MediaType.APPLICATION_JSON )
  @Override
  public Response queryDataservice( @PathParam( "dataserviceName" ) String dataserviceName,
                                    IQuery query ) {
    try {

      String sqlQuery = "SELECT * FROM "+ dataserviceName;
      SQL sql = new SQL( sqlQuery );

      SafePipedStreams safePipedStreams = new SafePipedStreams();

      StringBuffer sb = new StringBuffer( );
      Thread threadRead = new Thread( () -> {
        try {
          byte[] readBytes = new byte[50000];
          new DataInputStream( safePipedStreams.in ).readFully( readBytes );
          sb.append( new String(readBytes) );
        } catch ( IOException e ) {
          e.printStackTrace();
        }
      } );
      threadRead.start();

      RowMetaInterface stepFields = new RowMeta(  );
      Thread threadExecute = new Thread( () -> {
        try {
          DataServiceExecutor.Builder builder = resolver.createBuilder( sql );
          DataServiceExecutor dataServiceExecutor = builder.build();
          DataOutputStream dataOutputStream = new DataOutputStream( safePipedStreams.out );
          dataServiceExecutor.executeQuery( dataOutputStream ).waitUntilFinished();
          RowMetaInterface stepFieldsInner = dataServiceExecutor.getGenTrans().getTransMeta().getStepFields( dataServiceExecutor.getResultStepName() );
          stepFields.addRowMeta( stepFieldsInner );
          safePipedStreams.out.flush();
        } catch ( IOException|KettleException e ) {
          e.printStackTrace();
        }
      } );
      threadExecute.start();

      Thread.sleep( 10000 );

      return Response.ok( sb.toString() ).build();

    } catch ( Exception e ) { //TODO review exceptions
      throw new RuntimeException( e );
    }
  }

  private static class SafePipedStreams {
    final PipedOutputStream out;
    final PipedInputStream in;
    private volatile boolean open = true;

    private SafePipedStreams() throws IOException {
      in = new PipedInputStream() {
        @Override public void close() throws IOException {
          ifOpen( () -> open = false );
          //super.close();
        }
      };
      out = new PipedOutputStream( in ) {
        @Override public void write( int b ) throws IOException {
          ifOpen( () -> super.write( b ) );
        }

        @Override public void write( byte[] b, int off, int len ) throws IOException {
          ifOpen( () -> super.write( b, off, len ) );
        }
      };
    }

    private synchronized void ifOpen( DataServiceEndpoint.SafePipedStreams.IOExceptionAction action )
      throws IOException {
      if ( open ) {
        action.call();
      }
    }

    private interface IOExceptionAction {
      void call() throws IOException;
    }
  }
}

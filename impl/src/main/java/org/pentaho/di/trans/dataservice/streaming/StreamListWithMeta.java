package org.pentaho.di.trans.dataservice.streaming;

import org.pentaho.di.core.row.RowMetaInterface;

public class StreamListWithMeta<T> extends StreamList<T> {
  private RowMetaInterface rowMeta;

  public RowMetaInterface getRowMeta() {
    return rowMeta;
  }

  public void setRowMeta( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
  }
}

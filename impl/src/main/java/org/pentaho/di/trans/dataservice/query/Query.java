/*
 * Copyright 2016-2017 Hitachi Vantara. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 */
package org.pentaho.di.trans.dataservice.query;

import org.pentaho.det.api.data.access.query.IQuery;
import org.pentaho.det.api.data.access.query.IQueryField;
import org.pentaho.det.api.data.access.query.IQuerySortField;
import org.pentaho.det.api.data.access.query.filter.IFilter;

import java.util.ArrayList;
import java.util.List;

public class Query implements IQuery {
  private final List<IQueryField> rowFields = new ArrayList<>();
  private final List<IQueryField> measureFields = new ArrayList<>();
  private final List<IQuerySortField> orderBy = new ArrayList<>();
  private IFilter filter;
  private boolean distinct;

  @Override
  public List<IQueryField> getRowFields() {
    return this.rowFields;
  }

  @Override
  public List<IQueryField> getMeasureFields() {
    return this.measureFields;
  }

  @Override
  public List<IQuerySortField> getOrderBy() {
    return this.orderBy;
  }

  @Override
  public IFilter getFilter() {
    return this.filter;
  }

  @Override
  public boolean getDistinct() {
    return this.distinct;
  }
}

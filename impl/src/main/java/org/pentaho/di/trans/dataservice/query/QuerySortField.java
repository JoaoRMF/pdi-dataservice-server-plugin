/*
 * Copyright 2016 - 2017 Hitachi Vantara. All rights reserved.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.pentaho.det.api.data.access.query.IQuerySortField;

public class QuerySortField implements IQuerySortField {
  private final String name;
  private final String formula;
  private final IQuerySortField.Direction direction;

  @JsonCreator
  public QuerySortField( @JsonProperty( "name" ) String name,
                         @JsonProperty( "formula" ) String formula,
                         @JsonProperty( "direction" ) IQuerySortField.Direction direction ) {
    this.name = name;
    this.formula = formula;
    this.direction = direction != null ? direction : Direction.ASC;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getFormula() {
    return this.formula;
  }

  @Override
  public Direction getDirection() {
    return this.direction;
  }
}

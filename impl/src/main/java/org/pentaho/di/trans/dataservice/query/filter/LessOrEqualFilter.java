/*!
 * Copyright 2017 Hitachi Vantara. All rights reserved.
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
 * limitations under the License.
 */
package org.pentaho.di.trans.dataservice.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.pentaho.det.api.data.access.query.filter.IPropertyFilter;

@JsonTypeName( "<=" )
public class LessOrEqualFilter implements IPropertyFilter {
  private String property;
  private Object value;

  @JsonCreator
  public LessOrEqualFilter( @JsonProperty( "p" ) String property, @JsonProperty( "v" ) Object value ) {
    this.property = property;
    this.value = value;
  }

  @Override
  public String getOperator() {
    return "<=";
  }

  @Override
  public String getProperty() {
    return this.property;
  }

  @Override
  public Object getValue() {
    return this.value;
  }
}


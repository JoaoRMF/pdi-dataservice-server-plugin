/*!
 * Copyright 2017 - 2018 Hitachi Vantara. All rights reserved.
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
import org.pentaho.det.api.data.access.query.filter.ILikeFilter;

@JsonTypeName( "like" )
public class LikeFilter implements ILikeFilter {
  private String property;
  private Object value;
  private boolean anchorStart;
  private boolean anchorEnd;
  private boolean isCaseInsensitive;

  @JsonCreator
  public LikeFilter( @JsonProperty( "p" ) String property, @JsonProperty( "v" ) Object value,
    @JsonProperty( "s" ) boolean anchorStart, @JsonProperty( "e" ) boolean anchorEnd,
    @JsonProperty( "ci" ) boolean isCaseInsensitive ) {
    this.property = property;
    this.value = value;
    this.anchorStart = anchorStart;
    this.anchorEnd = anchorEnd;
    this.isCaseInsensitive = isCaseInsensitive;
  }

  @Override
  public String getOperator() {
    return "like";
  }

  @Override
  public String getProperty() {
    return this.property;
  }

  @Override
  public Object getValue() {
    return this.value;
  }

  @Override
  public boolean getAnchorStart() {
    return this.anchorStart;
  }

  @Override
  public boolean getAnchorEnd() {
    return this.anchorEnd;
  }

  @Override
  public boolean getIsCaseInsensitive() {
    return this.isCaseInsensitive;
  }
}

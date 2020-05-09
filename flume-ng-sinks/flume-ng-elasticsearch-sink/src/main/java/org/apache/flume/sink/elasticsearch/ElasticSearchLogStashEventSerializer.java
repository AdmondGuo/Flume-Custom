/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Serialize flume events into the same format LogStash uses</p>
 *
 * This can be used to send events to ElasticSearch and use clients such as
 * Kabana which expect Logstash formated indexes
 *
 * <pre>
 * {
 *    "@timestamp": "2010-12-21T21:48:33.309258Z",
 *    "@tags": [ "array", "of", "tags" ],
 *    "@type": "string",
 *    "@source": "source of the event, usually a URL."
 *    "@source_host": ""
 *    "@source_path": ""
 *    "@fields":{
 *       # a set of fields for this event
 *       "user": "jordan",
 *       "command": "shutdown -r":
 *     }
 *     "@message": "the original plain-text message"
 *   }
 * </pre>
 *
 * If the following headers are present, they will map to the above logstash
 * output as long as the logstash fields are not already present.</p>
 *
 * <pre>
 *  timestamp: long -> @timestamp:Date
 *  host: String -> @source_host: String
 *  src_path: String -> @source_path: String
 *  type: String -> @type: String
 *  source: String -> @source: String
 * </pre>
 *
 * @see "https://github.com/logstash/logstash/wiki/logstash%27s-internal-message-format"
 */
public class ElasticSearchLogStashEventSerializer implements
    ElasticSearchEventSerializer {

  @Override
  public XContentBuilder getContentBuilder(Event event) throws IOException {
    XContentBuilder builder = jsonBuilder().startObject();
    appendHeaders(builder, event);
    appendBody(builder, event);
    builder.endObject();
    return builder;
  }

  private void appendBody(XContentBuilder builder, Event event)
      throws IOException, UnsupportedEncodingException {
    byte[] body = event.getBody();
    ContentBuilderUtil.appendField(builder, "message", body);
  }

  private void appendHeaders(XContentBuilder builder, Event event)
      throws IOException {
    Map<String, String> headers = Maps.newHashMap(event.getHeaders());

    String timestamp = headers.get("timestamp");
    headers.remove("timestamp");
    if (!StringUtils.isBlank(timestamp)
        && StringUtils.isBlank(headers.get("@timestamp"))) {
      long timestampMs = Long.parseLong(timestamp);
      builder.field("@timestamp", new Date(timestampMs));    //想加在source下就用buildfield
    }
//    String tags = headers.get("tags");
//    if (!StringUtils.isBlank(timestamp)) {
////      String tagspure = tags.substring(1,tags.length()-1);
//      String temp = tags.replace("[","{");
//      String tagspure = temp.replace("]","}");
////      String [] taglist = tagspure.split(",");
//      builder.field("tags", tagspure);    //想加在source下就用buildfield
//    }



//    String input = headers.get("input");
//    if (!StringUtils.isBlank(input)) {
//      ContentBuilderUtil.appendField(builder, "input",      //想加在field下使用appendField
//          input.getBytes(charset));
//    }
//
//    String tags = headers.get("tags");
//    if (!StringUtils.isBlank(tags)) {
//      ContentBuilderUtil.appendField(builder, "tags", tags.getBytes(charset));
//    }
//
//    String host = headers.get("host");
//    if (!StringUtils.isBlank(host)) {
//      ContentBuilderUtil.appendField(builder, "host",
//          host.getBytes(charset));
//    }
//
//    String log = headers.get("log");
//    if  (!StringUtils.isBlank(host)) {
//      ContentBuilderUtil.appendField(builder, "log",
//              log.getBytes());
//    }
//
//    String agent = headers.get("agent");
//    if (!StringUtils.isBlank(agent)) {
//      ContentBuilderUtil.appendField(builder, "agent", agent.getBytes(charset));
//    }

    /*全部的headers加入到json中*/
    for (String key : headers.keySet()) {
      byte[] val = headers.get(key).getBytes(charset);
      ContentBuilderUtil.appendField(builder, key, val);
    }
  }

  @Override
  public void configure(Context context) {
    // NO-OP...
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    // NO-OP...
  }
}

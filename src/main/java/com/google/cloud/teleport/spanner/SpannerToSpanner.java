/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import com.google.cloud.spanner.Mutation;

public class SpannerToSpanner {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerToSpanner.class);

    /** Options for {@link TextImportPipeline}. */
    public interface Options extends PipelineOptions {
        @Description("Source Project ID of GCP Project")
        @Validation.Required
        ValueProvider<String> getSourceProjectId();

        void setSourceProjectId(ValueProvider<String> value);

        @Description("Destination Project ID of GCP Project")
        @Validation.Required
        ValueProvider<String> getDestinationProjectId();

        void setDestinationProjectId(ValueProvider<String> value);

        @Description("Instance ID to write to Spanner")
        @Validation.Required
        ValueProvider<String> getSourceInstanceId();

        void setSourceInstanceId(ValueProvider<String> value);

        @Description("Database ID to write to Spanner")
        @Validation.Required
        ValueProvider<String> getSourceDatabaseId();

        void setSourceDatabaseId(ValueProvider<String> value);

        @Description("Instance ID to write to Spanner")
        @Validation.Required
        ValueProvider<String> getDestinationInstanceId();

        void setDestinationInstanceId(ValueProvider<String> value);

        @Description("Database ID to write to Spanner")
        @Validation.Required
        ValueProvider<String> getDestinationDatabaseId();

        void setDestinationDatabaseId(ValueProvider<String> value);

        @Description("Spanner host. The default value is https://batch-spanner.googleapis.com.")
        @Default.String("https://batch-spanner.googleapis.com")
        ValueProvider<String> getSpannerHost();

        void setSpannerHost(ValueProvider<String> value);

        @Description("Spanner table name to query from")
        @Validation.Required
        String getSourceTable();

        void setSourceTable(String value);

        @Description("Spanner table to insert into")
        @Validation.Required
        String getDestinationTable();

        void setDestinationTable(String value);

        @Description("If true, wait for job finish. The default value is true.")
        @Default.Boolean(true)
        boolean getWaitUntilFinish();

        void setWaitUntilFinish(boolean value);

        @Description("If true, wait for job finish. The default value is true.")
        @Validation.Required
        Integer getNumberOfShards();

        void setNumberOfShards(Integer value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        p.apply("ReadFromSpannerSource",
            SpannerIO.read()
                    .withProjectId(options.getSourceProjectId())
                    .withInstanceId(options.getSourceInstanceId())
                    .withDatabaseId(options.getSourceDatabaseId())
                    .withQuery("SELECT UUID, Data, SortingKey, Timestamp FROM " + options.getSourceTable()))
            //Mutate data into destination schema, adding shardId to decrease hotspots
            .apply("MutateToDestinationSchema", ParDo.of(new DoFn<Struct, Mutation>() {
                Integer numberOfShards = options.getNumberOfShards();
                String destinationTable = options.getDestinationTable();
                @ProcessElement
                public void processElement(ProcessContext c) {
                    Struct currentRecord = c.element();
                    Integer primaryKeyHash = currentRecord.getString("Uuid").hashCode(); //Long.toString(currentRecord.getLong("UUID")).hashCode();
                    Integer hashModulo = primaryKeyHash% numberOfShards;

                    c.output(Mutation.newInsertBuilder(destinationTable)
                            .set("ShardId").to(hashModulo)
                            .set("OperationType").to("Insert")
                            .set("UUID").to(currentRecord.getLong("UUID"))
                            .set("Data").to(currentRecord.getString("Data"))
                            .set("SortingKey").to(currentRecord.getString("SortingKey"))
                            .set("Timestamp").to(currentRecord.getTimestamp("Timestamp"))
                            .build());
                }
            }))
            // Finally write the Mutations to destination Spanner table
            .apply("WriteToSpannerDestination", SpannerIO.write()
                    .withProjectId(options.getDestinationProjectId())
                    .withInstanceId(options.getDestinationInstanceId())
                    .withDatabaseId(options.getDestinationDatabaseId()));
    }
}

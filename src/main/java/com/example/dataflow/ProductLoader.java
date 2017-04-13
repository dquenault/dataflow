package com.example.dataflow;


import com.google.api.services.datastore.DatastoreV1;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.*;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import com.google.cloud.dataflow.sdk.options.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import javax.annotation.Nullable;

import static com.google.api.services.datastore.client.DatastoreHelper.makeKey;


/**
 * Created by davidq on 20/08/15.
 */

public class ProductLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ProductLoader.class);



    /** A DoFn that tokenizes comma separated lines into individual fields. */
    @SuppressWarnings("serial")
    static class TokenizesMessage extends DoFn<String, String[]> {
        @Override
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split("\\|",-1);
            c.output(fields);
        }
    }


    /**
     * A helper function to create the ancestor key for all created and queried entities.
     *
     * <p>We use ancestor keys and ancestor queries for strong consistency. See
     * {@link DatastoreWordCount} javadoc for more information.
     */
    static DatastoreV1.Key makeAncestorKey(@Nullable String namespace, String kind) {
        DatastoreV1.Key.Builder keyBuilder = makeKey(kind, "root");
        if (namespace != null) {
            keyBuilder.getPartitionIdBuilder().setNamespace(namespace);
        }
        return keyBuilder.build();
    }

    /**
     * A DoFn that creates entity for every line in Shakespeare.
     */
    static class CreateEntityFn extends DoFn<String, DatastoreV1.Entity> {
        private final String namespace;
        private final String kind;
        private final DatastoreV1.Key ancestorKey;

        CreateEntityFn(String namespace, String kind) {
            this.namespace = namespace;
            this.kind = kind;

            // Build the ancestor key for all created entities once, including the namespace.
            ancestorKey = makeAncestorKey(namespace, kind);
        }

        public DatastoreV1.Entity makeEntity(String content) {
            DatastoreV1.Entity.Builder entityBuilder = DatastoreV1.Entity.newBuilder();

            // All created entities have the same ancestor Key.
            DatastoreV1.Key.Builder keyBuilder = makeKey(ancestorKey, kind, UUID.randomUUID().toString());
            // NOTE: Namespace is not inherited between keys created with DatastoreHelper.makeKey, so
            // we must set the namespace on keyBuilder. TODO: Once partitionId inheritance is added,
            // we can simplify this code.
            if (namespace != null) {
                keyBuilder.getPartitionIdBuilder().setNamespace(namespace);
            }

            entityBuilder.setKey(keyBuilder.build());
            entityBuilder.addProperty(DatastoreV1.Property.newBuilder().setName("content")
                    .setValue(DatastoreV1.Value.newBuilder().setStringValue(content)));
            return entityBuilder.build();
        }

        @Override
        public void processElement(ProcessContext c) {
            c.output(makeEntity(c.element()));
        }
    }




    private static interface Options extends PipelineOptions {
        @Description("Path of the file to read from and store to Datastore")
        @Default.String("gs://prodinput/item_master.txt")
        String getInput();
        void setInput(String value);

        @Description("Dataset entity kind")
        @Default.String("Products")
        String getKind();
        void setKind(String value);

        @Description("Dataset namespace")
        String getNamespace();
        void setNamespace(@Nullable String value);

        @Description("Dataset ID to write to datastore")
        @Validation.Required
        String getDataset();
        void setDataset(String value);

        @Description("Read an existing dataset, do not write first")
        boolean isReadOnly();
        void setReadOnly(boolean value);

        @Description("Number of output shards")
        @Default.Integer(0) // If the system should choose automatically.
        int getNumShards();
        void setNumShards(int value);
    }
    /**
     * An example that creates a pipeline to populate DatastoreIO from a
     * text input.  Forces use of DirectPipelineRunner for local execution mode.
     */
    public static void writeDataToDatastore(Options options) {

        Pipeline p = Pipeline.create(options);
        p.apply(TextIO.Read.named("ReadFile").from(options.getInput()))
                .apply(ParDo.of(new CreateEntityFn(options.getNamespace(), options.getKind())))
                .apply(DatastoreIO.writeTo(options.getDataset()));

        p.run();
    }

    public static void main(String args[]) {
        // The options are used in two places, for Dataflow service, and
        // building DatastoreIO.Read object
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        // NOTE: this write does not delete any existing Entities in the Datastore, so if run
        // multiple times with the same output dataset, there may be duplicate entries. The
        // Datastore Query tool in the Google Developers Console can be used to inspect or erase all
        // entries with a particular namespace and/or kind.
        ProductLoader.writeDataToDatastore(options);

    }
}

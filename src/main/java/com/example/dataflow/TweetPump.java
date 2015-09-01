package com.example.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.windowing.CalendarWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by davidq on 06/07/15.
 */

public class TweetPump {
/**

 * <p> To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 * and an output prefix on GCS:
 * <pre>{@code
 *   --output=gs://YOUR_OUTPUT_PREFIX
 * }</pre>
 *
 * <p> The default input is {@code gs://dataflow-samples/wikipedia_edits/*.json} and can be
 * overridden with {@code --input}.
 *
 * <p> The input for this example is large enough that it's a good place to enable (experimental)
 * autoscaling:
 * <pre>{@code
 *   --autoscalingAlgorithm=BASIC
 *   --maxNumWorkers=20
 * }
 * </pre>
 * This will automatically scale the number of workers up over time until the job completes.
 */

    private static final String READ_TOPIC ="/topics/tweets";

    /**
     * Extracts user and timestamp from a TableRow representing a Wikipedia edit.
     */
    static class ExtractUserAndTimestamp extends DoFn<TableRow, String> {
        private static final long serialVersionUID = 0;

        @Override
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            int timestamp = (Integer) row.get("timestamp");
            String userName = (String) row.get("contributor_username");
            if (userName != null) {
                // Sets the implicit timestamp field to be used in windowing.
                c.outputWithTimestamp(userName, new Instant(timestamp * 1000L));
            }
        }
    }

    /**
     * Computes the number of edits in each user session.  A session is defined as
     * a string of edits where each is separated from the next by less than an hour.
     */
    static class ComputeSessions
            extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        private static final long serialVersionUID = 0;

        @Override
        public PCollection<KV<String, Long>> apply(PCollection<String> actions) {
            return actions
                    .apply(Window.<String>into(Sessions.withGapDuration(Duration.standardHours(1))))

                    .apply(Count.<String>perElement());
        }
    }

    /**
     * Computes the longest session ending in each month.
     */
    private static class TopPerMonth
            extends PTransform<PCollection<KV<String, Long>>, PCollection<List<KV<String, Long>>>> {
        private static final long serialVersionUID = 0;

        @Override
        public PCollection<List<KV<String, Long>>> apply(PCollection<KV<String, Long>> sessions) {
            return sessions
                    .apply(Window.<KV<String, Long>>into(CalendarWindows.months(1)))

                    .apply(Top.of(1, new SerializableComparator<KV<String, Long>>() {
                        private static final long serialVersionUID = 0;

                        @Override
                        public int compare(KV<String, Long> o1, KV<String, Long> o2) {
                            return Long.compare(o1.getValue(), o2.getValue());
                        }
                    }).withoutDefaults());
        }
    }

    static class SessionsToStringsDoFn extends DoFn<KV<String, Long>, KV<String, Long>>
            implements DoFn.RequiresWindowAccess {

        private static final long serialVersionUID = 0;

        @Override
        public void processElement(ProcessContext c) {
            c.output(KV.of(
                    c.element().getKey() + " : " + c.window(), c.element().getValue()));
        }
    }

    static class FormatOutputDoFn extends DoFn<List<KV<String, Long>>, String>
            implements DoFn.RequiresWindowAccess {
        private static final long serialVersionUID = 0;

        @Override
        public void processElement(ProcessContext c) {
            for (KV<String, Long> item : c.element()) {
                String session = item.getKey();
                long count = item.getValue();
                c.output(session + " : " + count + " : " + ((IntervalWindow) c.window()).start());
            }
        }
    }

    static class ComputeTopSessions extends PTransform<PCollection<TableRow>, PCollection<String>> {

        private static final long serialVersionUID = 0;

        private final double samplingThreshold;

        public ComputeTopSessions(double samplingThreshold) {
            this.samplingThreshold = samplingThreshold;
        }

        @Override
        public PCollection<String> apply(PCollection<TableRow> input) {
            return input
                    .apply(ParDo.of(new ExtractUserAndTimestamp()))

                    .apply(ParDo.named("SampleUsers").of(
                            new DoFn<String, String>() {
                                private static final long serialVersionUID = 0;

                                @Override
                                public void processElement(ProcessContext c) {
                                    if (Math.abs(c.element().hashCode()) <= Integer.MAX_VALUE * samplingThreshold) {
                                        c.output(c.element());
                                    }
                                }
                            }))

                    .apply(new ComputeSessions())

                    .apply(ParDo.named("SessionsToStrings").of(new SessionsToStringsDoFn()))
                    .apply(new TopPerMonth())
                    .apply(ParDo.named("FormatOutput").of(new FormatOutputDoFn()));
        }
    }

    /**
     * Options supported by this class.
     */
    private static interface Options extends PipelineOptions
    {
        @Description("Topic to read from")
        @Default.String(READ_TOPIC)
        String getInput();
        void setInput(String value);

        @Description("File to output results to")
        @Validation.Required
        String getOutput();
        void setOutput(String value);
    }

    /**
     * Defines the BigQuery schema used for the output.
     */
    static TableSchema getSchema() {
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        fields.add(new TableFieldSchema().setName("tag").setType("STRING"));
        fields.add(new TableFieldSchema().setName("tweet").setType("STRING"));
        fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);

        Pipeline p = Pipeline.create(dataflowOptions);

        double samplingThreshold = 0.1;

        p.apply(TextIO.Read
                .from(options.getInput())
                .withCoder(TableRowJsonCoder.of()))
                .apply(new ComputeTopSessions(samplingThreshold))
                .apply(TextIO.Write.named("Write").withoutSharding().to(options.getOutput()));

        p.run();
    }



//    if (options.isStreaming()) {
//        // In order to cancel the pipelines automatically,
//        // {@literal DataflowPipelineRunner} is forced to be used.
//        options.setRunner(DataflowPipelineRunner.class);
//    }
//    options.setBigQuerySchema(FormatStatsFn.getSchema());
//    // Using DataflowExampleUtils to set up required resources.
//    DataflowExampleUtils dataflowUtils = new DataflowExampleUtils(options);
//    dataflowUtils.setup();
//
//    Pipeline pipeline = Pipeline.create(options);
//    TableReference tableRef = new TableReference();
//    tableRef.setProjectId(options.getProject());
//    tableRef.setDatasetId(options.getBigQueryDataset());
//    tableRef.setTableId(options.getBigQueryTable());
//
//    PCollection<KV<String, StationSpeed>> input;
//    if (options.isStreaming()) {
//        input = pipeline
//                .apply(PubsubIO.Read.topic(options.getPubsubTopic()))
//                        // row... => <station route, station speed> ...
//                .apply(ParDo.of(new ExtractStationSpeedFn(false /* outputTimestamp */)));
//    } else {
//        input = pipeline
//                .apply(TextIO.Read.from(options.getInputFile()))
//                .apply(ParDo.of(new ExtractStationSpeedFn(true /* outputTimestamp */)));
//    }
//
//    // map the incoming data stream into sliding windows.
//    // The default window duration values work well if you're running the accompanying Pub/Sub
//    // generator script without the --replay flag, so that there are no simulated pauses in
//    // the sensor data publication. You may want to adjust the values otherwise.
//    input.apply(Window.<KV<String, StationSpeed>>into(SlidingWindows.of(
//            Duration.standardMinutes(options.getWindowDuration())).
//    every(Duration.standardMinutes(options.getWindowSlideEvery()))))
//            .apply(new TrackSpeed())
//            .apply(BigQueryIO.Write.to(tableRef)
//    .withSchema(FormatStatsFn.getSchema()));
//
//    PipelineResult result = pipeline.run();
//    if (options.isStreaming() && !options.getInputFile().isEmpty()) {
//        // Inject the data into the Pub/Sub topic with a Dataflow batch pipeline.
//        dataflowUtils.runInjectorPipeline(options.getInputFile(), options.getPubsubTopic());
//    }
//
//    // dataflowUtils will try to cancel the pipeline and the injector before the program exists.
//    dataflowUtils.waitToFinish(result);
}



package com.example.dataflow;

        import java.io.BufferedReader;
        import java.io.FileNotFoundException;
        import java.io.FileReader;
        import java.io.Serializable;
        import java.text.DateFormat;
        import java.text.SimpleDateFormat;
        import java.util.ArrayList;
        import java.util.List;

        import com.google.cloud.dataflow.sdk.options.Description;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import com.google.api.services.bigquery.model.TableFieldSchema;
        import com.google.api.services.bigquery.model.TableRow;
        import com.google.api.services.bigquery.model.TableSchema;
        import com.google.cloud.dataflow.sdk.Pipeline;
        import com.google.cloud.dataflow.sdk.io.BigQueryIO;
        import com.google.cloud.dataflow.sdk.io.TextIO;
        import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
        import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
        import com.google.cloud.dataflow.sdk.options.PipelineOptions;
        import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
        import com.google.cloud.dataflow.sdk.transforms.Aggregator;
        import com.google.cloud.dataflow.sdk.transforms.DoFn;
        import com.google.cloud.dataflow.sdk.transforms.ParDo;
        import com.google.cloud.dataflow.sdk.transforms.Sum;


/**
 * Created by davidq on 20/08/15.
 */

public class FilePump {
    private static final Logger LOG = LoggerFactory.getLogger(FilePump.class);


    static class Record implements Serializable {
        List<String> fields = new ArrayList<String>();
    }

    static class SchemaFields implements Serializable {
        List<String> fields = new ArrayList<String>();
    }


    /** A DoFn that tokenizes comma separated lines into individual fields. */
    @SuppressWarnings("serial")
    static class TokenizesMessage extends DoFn<String, String[]> {
        @Override
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split("\\|",-1);
            c.output(fields);
        }
    }


    /** A DoFn that creates a simple Record object. */
    @SuppressWarnings("serial")
    static class CreateRecords extends DoFn<String[], Record> {
        private Aggregator<Integer,Integer> myAggregator;

        DateFormat df = new SimpleDateFormat("YYYYMMdd");
        DateFormat dfGoogle = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");
        Integer schemaDataSize = 0;
        SchemaFields newSchema = null;

        public CreateRecords (SchemaFields newSchema) {
            this.newSchema = newSchema;
            this.schemaDataSize = newSchema.fields.size();
        }

        /** Here we are creating a global aggregator */
        @Override
        public void startBundle(Context c) {
            myAggregator = createAggregator("bad_records",
                    new Sum.SumIntegerFn());
        }

        /** Process the tokenized fields into a record, done for code clarity */
        @Override
        public void processElement(ProcessContext c) {
            String[] fields = c.element();

            if (fields.length != schemaDataSize) {

                LOG.info("bad record - wrong number of fields, no of fields = " + fields.length +
                        " schema fields = " + schemaDataSize);
                recordBadrecord(fields);

            } else {

                try {

                    Record record = new Record();

                    for (int i =0;i<schemaDataSize;i++) {
                        record.fields.add(fields[i].toString());
                    }
                    c.output(record);

                } catch (NumberFormatException e) {

                    recordBadrecord(fields);

                    LOG.error(e.getMessage());
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    recordBadrecord(fields);
                    LOG.error(e.getMessage());
                }
            }
        }

        /** If any bad records push to the counter */
        public void recordBadrecord(String[] str) {
            // TODO Add counter code
            //myAggregator.addValue(1);
        }
    }


    /** Create a TableRow ready for push into BQ */
    @SuppressWarnings("serial")
    static class CreateTableRow extends DoFn<Record, TableRow> {


        Integer schemaDataSize = 0;
        SchemaFields schemaData = null;

        public CreateTableRow (SchemaFields schemaData) {
            this.schemaData = schemaData;
            this.schemaDataSize = schemaData.fields.size();
        }

        @Override
        public void processElement(DoFn<Record, TableRow>.ProcessContext c)
                throws Exception {

            String fieldName = "";
            String dataField = "";
            TableRow row = new TableRow();


            if (schemaDataSize!=43) LOG.info("bqSchemaSize: " + schemaDataSize);

            for (int i=0; i<schemaDataSize; i++) {
                try {
                    dataField = c.element().fields.get(i);
                    fieldName = schemaData.fields.get(i);
                   // LOG.info("Field: "+ fieldName + " datafield: " +dataField );

                    row.set(fieldName, dataField);
                } catch (NullPointerException e) {
                    LOG.info("Field name is null at position: "+ i + " error: " +e );
                }
            }

            c.output(row);

        }

    }


    /** Define the schema to be used in BQ */
    public static TableSchema bqSchema;

    public static TableSchema getSchema(Options options, SchemaFields fieldList) {
        BufferedReader brSchemaFile;

        try {
            brSchemaFile = new BufferedReader(
                    new FileReader(options.getSchemaFile()));
        } catch (FileNotFoundException e) {
            LOG.error(e.toString());
            return null;
        }

        String text = new String();
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();


        try {
            text = brSchemaFile.readLine();
        } catch (Exception e) {
            LOG.error(e.toString());
        }

        String[] strArray = text.split(",");
        for (String fieldNameType : strArray) {
            String[] definition = fieldNameType.split(":");
            fields.add(new TableFieldSchema().setName(definition[0]).setType(definition[1]));
            fieldList.fields.add(definition[0]);
        }

        return new TableSchema().setFields(fields);

    }



    /** Badly done way to deal with Options, please look at GitHub Dataflow examples */
    private static interface Options extends PipelineOptions {
        @Description("")
        String getInput();
        void setInput(String value);

        @Description("")
        String getOutput();
        void setOutput(String value);

        @Description("Schema file for bigquery")
        String getSchemaFile();
        void setSchemaFile(String value);

        @Description("BigQuery Project")
        String getbqProject();
        void setbqProject(String value);

        @Description("BigQuery dataset name")
        String getbqDataSet();
        void setbqDataSet(String value);

        @Description("BigQuery table name")
        String getbqTableName();
        void setbqTableName(String value);
    }

    /** Main pipeline */
    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(Options.class);

        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);

		/*
		 * Set Worker configurations
		 * Allow Autoscaling but set maximum to be 10 workers
		 */
        dataflowOptions.setAutoscalingAlgorithm(AutoscalingAlgorithmType.BASIC);
        dataflowOptions.setMaxNumWorkers(7); //Number of workers

        Pipeline p = Pipeline.create(dataflowOptions);

        //Create field serialised object
        SchemaFields fieldList = new SchemaFields();

        bqSchema = getSchema(options, fieldList); //Construct the schema locally and populate fieldlist

        LOG.info(bqSchema.toString());
		/*
		 * Do the main pipeline work
		 */
        try {
            p.apply(TextIO.Read.named("Reader")
                    .from(options.getInput())
                    .withCompressionType(TextIO.CompressionType.GZIP))
                    .apply(ParDo.of(new TokenizesMessage()))
                    .apply(ParDo.of(new CreateRecords(fieldList)))
                    .apply(ParDo.of(new CreateTableRow(fieldList)))
                    .apply(BigQueryIO.Write
                            .to(options.getbqProject() + ":" + options.getbqDataSet() + "." + options.getbqTableName())
                            .withSchema(bqSchema)
                            .withCreateDisposition(
                                    BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(
                                    BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

            p.run();
        }catch (Exception e) {
            System.out.println("Error: "+ e);
        }

    }
}

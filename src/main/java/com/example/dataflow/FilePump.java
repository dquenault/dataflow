package com.example.dataflow;

        import java.io.*;
        import java.text.DateFormat;
        import java.text.SimpleDateFormat;
        import java.util.ArrayList;
        import java.util.List;


        import org.apache.beam.sdk.Pipeline;
        import org.apache.beam.sdk.io.Compression;
        import org.apache.beam.sdk.io.TextIO;
        import org.apache.beam.sdk.options.Description;
        import org.apache.beam.sdk.options.PipelineOptions;
        import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
        import org.apache.beam.sdk.options.PipelineOptionsFactory;
        import org.apache.beam.sdk.transforms.DoFn;
        import org.apache.beam.sdk.transforms.ParDo;
        import org.apache.beam.sdk.values.PCollection;
        import org.apache.logging.log4j.Logger;
        import org.apache.logging.log4j.LogManager;


        import com.google.api.services.bigquery.model.TableRow;
        import com.google.api.services.bigquery.model.TableSchema;
        import com.google.api.services.bigquery.model.TableFieldSchema;

/**
 * Created by davidq on 20/08/15.
 */

public class FilePump {
    private static final Logger LOG = LogManager.getLogger(FilePump.class);


    static class Record implements Serializable {
        List<String> fields = new ArrayList<String>();
    }

    static class SchemaFields implements Serializable {
        List<String> fields = new ArrayList<String>();
    }


    /** A DoFn that tokenizes comma separated lines into individual fields. */
    @SuppressWarnings("serial")
    static class TokenizesMessage extends DoFn<String, String[]> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split("\\|",-1);
            c.output(fields);
        }
    }


    /** A DoFn that creates a simple Record object. */
    @SuppressWarnings("serial")
    static class CreateRecords extends DoFn<String[], Record> {

        DateFormat df = new SimpleDateFormat("YYYYMMdd");
        DateFormat dfGoogle = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");
        Integer schemaDataSize = 0;
        SchemaFields newSchema = null;

        public CreateRecords (SchemaFields newSchema) {
            this.newSchema = newSchema;
            this.schemaDataSize = newSchema.fields.size();
        }


        /** Process the tokenized fields into a record, done for code clarity */
        @ProcessElement
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

        @ProcessElement
        public void processElement(DoFn<Record, TableRow>.ProcessContext c)
                throws Exception {

            String fieldName = "";
            String dataField = "";
            TableRow row = new TableRow();

            for (int i=0; i<schemaDataSize; i++) {
                try {
                    dataField = c.element().fields.get(i);
                    fieldName = schemaData.fields.get(i);

                    row.set(fieldName, dataField);
                } catch (NullPointerException e) {
                    LOG.info("Field name is null at position: "+ i + " error: " +e );
                }
            }

            c.output(row);

        }

    }

    public static TableSchema getSchema(Options options, SchemaFields fieldList) {
        BufferedReader brSchemaFile;

        try {
            brSchemaFile = new BufferedReader(
                    new FileReader(options.getSchemaFile()));
        } catch (Exception e) {
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


    private static interface Options extends PipelineOptions {
        //Configuration of custom options

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

    /** Main pipeline class */
    public static void main(String[] args) {

        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(Options.class);

        Pipeline p = Pipeline.create(options);

        SchemaFields fieldList = new SchemaFields();        //Create field serialised object
        TableSchema bqSchema = getSchema(options, fieldList); //Construct the schema locally and populate fieldlist

        LOG.info(bqSchema.toString());

        try {
            PCollection<TableRow> output = p.apply("Reader", TextIO.read().from(options.getInput())
                    .withCompression(Compression.AUTO))
                    .apply(ParDo.of(new TokenizesMessage()))
                    .apply(ParDo.of(new CreateRecords(fieldList)))
                    .apply(ParDo.of(new CreateTableRow(fieldList)));

            output.apply("Write to BQ", BigQueryIO.writeTableRows()
                    .to(options.getbqProject() + ":" + options.getbqDataSet() + "." + options.getbqTableName())
                    .withSchema(bqSchema)
                    .withCreateDisposition( BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

            //output.apply("Write_To_File", TextIO.write().to(options.getOutput()));

            p.run();
        } catch (Exception e) {
            System.out.println("Error: "+ e);
        }

    }
}

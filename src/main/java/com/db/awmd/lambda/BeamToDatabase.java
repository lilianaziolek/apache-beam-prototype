package com.db.awmd.lambda;

import org.apache.beam.sdk.io.CsvIO;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.csv.CSVRecord;

public class BeamToDatabase {

    public static class CsvRecordToBqTableRow extends DoFn<String, TableRow> {

    }

    //    public static class Read extends PTransform<PBegin, PCollection<CSVRecord>> {
    //
    //        @Override
    //        public PCollection<CSVRecord> expand(PBegin input) {
    //            input.
    //        }
    //    }

    public static void main(String[] args) {
        /*
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline p = Pipeline.create(options);

        PCollection<CSVRecord> linesInFile = p.apply(Csv2IO.read().from("gs://dataflow-db-poc-lili/unzipped/Curve - US CMBS Agency___10_31_2017.csv"));
        //TextIO.Read.from(options.getSource()).named("ReadFileData")
        linesInFile.apply("splitLine", ParDo.of(new DoFn<CSVRecord, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Map<String, String> csvMap = c.element().toMap();
                c.output(csvMap.toString());
            }
        })).apply(TextIO.write().to("gs://dataflow-db-poc-lili/beam/output/splitCsvCurveFromRecords.csv"));

        p.run().waitUntilFinish();
         */
        DirectRunner directRunner = DirectRunner.fromOptions(PipelineOptionsFactory.fromArgs(args).create());
        //        Pipeline p = Pipeline.create();
        //        p.apply("ReadLines", TextIO.read().from("D:\\src\\GCP DB PoC\\apache-beam-prototype\\src\\main\\resources\\sample.csv"))
        //                .apply("Print", ParDo.of(new DoFn<String, String>() {
        //                    @ProcessElement
        //                    public void processElement(ProcessContext pc) {
        //                        System.out.println(pc.element());
        //                        pc.output("New " + pc.element());
        //                    }
        //                }));
        //        directRunner.run(p);

        Pipeline p2 = Pipeline.create();
        p2.apply("ReadRecords", CsvIO.read().from("D:\\src\\GCP DB PoC\\apache-beam-prototype\\src\\main\\resources\\sample.csv"))
                .apply("Print", ParDo.of(new DoFn<CSVRecord, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext pc) {
                        CSVRecord element = pc.element();
                        System.out.println("RECORD: " + element.get("col1"));
                        //                        System.out.println(element);
                        pc.output("New " + element);
                    }
                }));
        directRunner.run(p2);
    }
}

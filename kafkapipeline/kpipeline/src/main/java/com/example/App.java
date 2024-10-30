package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.direct.DirectRunner;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setRunner(DirectRunner.class);

        Pipeline p = Pipeline.create(options);
        p.apply(new TestConsumer());

        p.run();

    }
}

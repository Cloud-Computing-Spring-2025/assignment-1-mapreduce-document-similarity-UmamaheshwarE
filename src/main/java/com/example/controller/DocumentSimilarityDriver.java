package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.example.DocumentSimilarityMapper;
import com.example.DocumentSimilarityReducer;

public class DocumentSimilarityDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DocumentSimilarityDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration configuration = new Configuration();
        FileSystem hdfsFileSystem = FileSystem.get(configuration);

        // Ensure previous output directory does not exist
        Path resultPath = new Path(args[1]);

        if (hdfsFileSystem.exists(resultPath)) {
            hdfsFileSystem.delete(resultPath, true);
            System.out.println("Deleted existing output directory: " + args[1]);
        }

        // Single MapReduce Job: Compute document similarity
        System.out.println("Starting Document Similarity MapReduce Job...");
        Job similarityJob = Job.getInstance(configuration, "Document Similarity");
        similarityJob.setJarByClass(DocumentSimilarityDriver.class);
        similarityJob.setMapperClass(DocumentSimilarityMapper.class);
        similarityJob.setReducerClass(DocumentSimilarityReducer.class);

        similarityJob.setOutputKeyClass(Text.class);
        similarityJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(similarityJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(similarityJob, resultPath);

        System.exit(similarityJob.waitForCompletion(true) ? 0 : 1);
    }
}
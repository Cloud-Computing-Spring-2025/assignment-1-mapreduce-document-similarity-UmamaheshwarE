package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class DocumentSimilarityMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object recordKey, Text recordValue, Context jobContext) throws IOException, InterruptedException {
        String inputLine = recordValue.toString();
        String[] dataSegments = inputLine.split("\\s+", 2);

        if (dataSegments.length < 2)
            return; // Ignore incorrectly formatted lines

        String docIdentifier = dataSegments[0];
        String docContent = dataSegments[1];

        HashSet<String> wordSet = new HashSet<>();
        StringTokenizer wordTokenizer = new StringTokenizer(docContent);

        while (wordTokenizer.hasMoreTokens()) {
            wordSet.add(wordTokenizer.nextToken().toLowerCase());
        }

        // Emit (word, documentID) for correct grouping in the reducer
        for (String uniqueWord : wordSet) {
            jobContext.write(new Text(uniqueWord), new Text(docIdentifier));
        }
    }
}
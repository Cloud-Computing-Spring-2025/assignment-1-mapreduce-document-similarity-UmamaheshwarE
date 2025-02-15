package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, Text> {

    private final Map<String, Set<String>> docWordMapping = new HashMap<>();

    @Override
    public void reduce(Text word, Iterable<Text> docIdentifiers, Context jobContext)
            throws IOException, InterruptedException {
        List<String> documentList = new ArrayList<>();

        for (Text doc : docIdentifiers) {
            String docName = doc.toString();
            documentList.add(docName);

            // Maintain word mapping for each document
            docWordMapping.putIfAbsent(docName, new HashSet<>());
            docWordMapping.get(docName).add(word.toString());
        }
    }

    @Override
    protected void cleanup(Context jobContext) throws IOException, InterruptedException {
        List<String> documentNames = new ArrayList<>(docWordMapping.keySet());

        // Compare document pairs and calculate Jaccard Similarity
        for (int i = 0; i < documentNames.size(); i++) {
            for (int j = i + 1; j < documentNames.size(); j++) {
                String firstDoc = documentNames.get(i);
                String secondDoc = documentNames.get(j);

                double similarityScore = calculateJaccardSimilarity(firstDoc, secondDoc);

                if (similarityScore > 0) {
                    String similarityOutputKey = "<" + firstDoc + ", " + secondDoc + ">";
                    String similarityOutputValue = " -> " + String.format("%.2f", similarityScore * 100) + "%";
                    jobContext.write(new Text(similarityOutputKey), new Text(similarityOutputValue));
                }
            }
        }
    }

    private double calculateJaccardSimilarity(String docOne, String docTwo) {
        Set<String> wordsInDocOne = docWordMapping.getOrDefault(docOne, new HashSet<>());
        Set<String> wordsInDocTwo = docWordMapping.getOrDefault(docTwo, new HashSet<>());

        Set<String> intersectionSet = new HashSet<>(wordsInDocOne);
        intersectionSet.retainAll(wordsInDocTwo); // Calculate |A ∩ B|

        Set<String> unionSet = new HashSet<>(wordsInDocOne);
        unionSet.addAll(wordsInDocTwo); // Calculate |A ∪ B|

        if (unionSet.isEmpty())
            return 0; // Prevent division by zero

        return (double) intersectionSet.size() / unionSet.size();
    }
}

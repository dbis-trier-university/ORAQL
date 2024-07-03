# ORAQL: An Overlap and Reliability Aware Query Processing Layer

## What is ORAQL?
The increasing numbers of available data sources have led to increased data redundancy and hence novel challenges for federations. Typically, federation engines query all endpoints that provide relevant data for a given query. However, considering the overlap, a subset of these sources might already be sufficient to obtain a complete answer. Further, we deliberately might not wish to include all sources in the evaluation and make a decision based the reliability of a source. We therefore present ORAQL, an approach that exploits statistics capturing the overlap between sources to choose a subset of the available sources in the federation to compute a _complete_ answer while _minimizing redundant answers_. Moreover, a user-provided reliability goal is taken into account. Hence, we propose an approach based on a majority vote over multiple sources to increase the reliability of the query result. For this work, we focus on TPF interfaces, since they are the least expressive interfaces and hence our approach can be adopted for more expressive interfaces, e.g. SPARQL endpoints. The presented methods to capture the overlap between sources of a federation have shown to generate useful overlap profiles with a maximum deviation of less than five percent. Even if the identification of redundant data is NP-hard we presented an approximation with a significant reduction in requested endpoints. Further, we have shown that ORAQL is granularly tunable towards reliability and can beat a state-of-the-art baseline system in terms of coverage and reliability.

## Datasets for Reproducible Evaluation
To simulate TPF interfaces and ensure a reproducibility we used the newest version of the [ETARA]{https://github.com/ETARA-Benchmark-System} benchmark system since it can be used to simulate TPF interfaces. 

As data sources we used different scholarly data sets based on dblp with data on publications between 2015 and 2020. All used datasets and queries for each experiment can be found [here](https://shorturl.at/fN067). We created seventeen data sets, each with different degrees of overlap. In addition, the datasets used for the reliability evaluation also contain different errors for titles and author names.

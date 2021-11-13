# Progressive and Incremental ER (PIER)
This is the source code for the framework proposed in the paper "Progressive Entity Resolution over Incremental Data".

#### Datasets
Datasets for 'dblp-acm' and 'movies' are in `data/cleanCleanErDatasets`. The datasets 'dbpedia' and 'synthetic' can be downloaded from this [Mendeley](https://data.mendeley.com/datasets/4whpm32y47/7) repository, used to assess [JedAI](https://github.com/scify/JedAIToolkit/) performance.
- dataset 'dbpedia': file newDBPedia.tar.xz in Mendeley's Real Clean-Clean ER data
- dataset 'synthetic': files 2MProfiles and 2MIdDuplicates in Mendeley's Synthetic Dirty ER data

To download additional datasets from JedAI you can run the `data/download.sh` script from inside the `data/` directory (svn required).

#### Requirements
The framework is written in [Scala](https://www.scala-lang.org/) (version 2.13.1) and it requires SBT and OpenJDK to be installed and executed.
- [SBT](https://www.scala-sbt.org/1.x/docs/Setup.html)
- [OpenJDK 11](https://openjdk.java.net/projects/jdk/11/)

Library dependencies are listed in the SBT configuration file `build.sbt`.

#### Installation
To install and download library dependencies:
```
./install.sh
```

#### Run
Clean-Clean ER. To run the framework for the 'movies' dataset (imdb-dbpedia) .
```
sbt "runMain AkkaProgressiveIncrementalCCMain -d1 imdb -d2 dbpedia -gt movies --pname ieps -m ed -bp 0.05 -bg 0.05 --batches 1000 --rate 100 --budget 2"
```

Dirty ER. To run the framework for the 'synthetic' dataset (2M).
```
sbt "runMain AkkaProgressiveIncrementalDirtyMain -d1 2M --gt 2M --pname ieps -m ed -bp 0.05 -bg 0.05 --batches 20000 --rate 32 --budget 60"
```

About the options:
- '-d1' specifies the first dataset.
- '-d2' specifies the second dataset.
- '-gt' specifies the groundtruth file.'
- '--pname' specifies the prioritization method (ieps -> I-PES, ipcs -> I-PCS, ipbs3 -> I-PBS).
- '-m' specifies the similarity function used (js -> Jaccard, ed -> Edit Distance).
- '-bp' and '-bg' specify the parameters for block pruning and block ghosting. For the dataset 'dbpedia' set `-bp 0.005`.
- '--batches' specifies the number of increments
- '--rate' specifies how many increments per second the system receives
- '--budget' specifies the budget in minutes

For larger datasets consider to increase the heap size. For example for 'dbpedia':
```
export SBT_OPTS="-Xmx40G"
```

#### Contact
For any problem contact me at leonardo.gazzarri@ipvs.uni-stuttgart.de

#!/bin/bash

echo "Run this script from data folder"

# HERE put links to retrieve datasets and groundtruth files from JedAI
echo "Retrieving Dirty Datasets..."
DIRTY_DATASETS=https://github.com/scify/JedAIToolkit/trunk/data/dirtyErDatasets
svn checkout ${DIRTY_DATASETS}

echo "Retrieving Clean Clean Datasets..."
CC_DATASETS=https://github.com/scify/JedAIToolkit/trunk/data/cleanCleanErDatasets
svn checkout ${CC_DATASETS}

DBPEDIA=https://drive.google.com/uc?id=1rFAwrmlsfR86fVIkOaGXCSzbfZIZufY2
echo "To retrieve large DBPEDIA you need to have installed gdown"
echo "To install it: pip install gdown"
read -p "Retrieve large DBPEDIA? y/n " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    gdown ${DBPEDIA}
fi
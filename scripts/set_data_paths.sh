#!/bin/bash

export VFENGINE_AMAZON0601_DATASET_PATH='/home/sunny/work/scratch/Amazon0601.txt'
export VFENGINE_AMAZON0601_SERIALIZED_READ_PATH='/home/sunny/work/scratch/'
export VFENGINE_AMAZON0601_SERIALIZED_WRITE_PATH='/home/sunny/work/scratch/'
echo "VFENGINE_AMAZON0601_DATASET_PATH is set to: $VFENGINE_AMAZON0601_DATASET_PATH"
echo "VFENGINE_AMAZON0601_SERIALIZED_READ_PATH is set to: $VFENGINE_AMAZON0601_SERIALIZED_READ_PATH"
echo "VFENGINE_AMAZON0601_SERIALIZED_WRITE_PATH is set to: $VFENGINE_AMAZON0601_SERIALIZED_WRITE_PATH"

#This script enables the data reading/writing path for the dataset, modify for your own systems
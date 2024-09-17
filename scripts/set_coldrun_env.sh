#!/bin/bash

export VFENGINE_AMAZON0601_DATASET_PATH='/home/sunny/work/scratch/Amazon0601.txt'
export VFENGINE_AMAZON0601_SERIALIZED_WRITE_PATH='/home/sunny/work/scratch/'
export VFENGINE_ENABLE_COLD_RUN=1
unset VFENGINE_DISABLE_DESERIALIZE
echo "VFENGINE_AMAZON0601_DATASET_PATH is set to: $VFENGINE_AMAZON0601_DATASET_PATH"
echo "VFENGINE_AMAZON0601_SERIALIZED_WRITE_PATH is set to: $VFENGINE_AMAZON0601_SERIALIZED_WRITE_PATH"
echo "VFENGINE_ENABLE_COLD_RUN is set to: $VFENGINE_ENABLE_COLD_RUN"
echo "VFENGINE_DISABLE_DESERIALIZE is unset to: $VFENGINE_DISABLE_DESERIALIZE"

#This script enables the cold run mode of the engine, data will be read from the CSV
#file, and then deserialization will occur
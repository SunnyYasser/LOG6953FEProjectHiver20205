#!/bin/bash

export VFENGINE_AMAZON0601_DATASET_PATH='/home/sunny/work/scratch/Amazon0601.txt'
export VFENGINE_AMAZON0601_SERIALIZED_READ_PATH='/home/sunny/work/scratch/'
export VFENGINE_ENABLE_HOT_RUN=1
unset VFENGINE_DISABLE_SERIALIZE
echo "VFENGINE_AMAZON0601_DATASET_PATH is set to: $AMAZON0601_DATASET_PATH"
echo "VFENGINE_AMAZON0601_SERIALIZED_WRITE_PATH is set to: $AMAZON0601_SERIALIZED_WRITE_PATH"
echo "VFENGINE_ENABLE_HOT_RUN is set to: $VFENGINE_ENABLE_HOT_RUN"
echo "VFENGINE_DISABLE_SERIALIZE is unset to: $VFENGINE_DISABLE_SERIALIZE"


#This script enables the hot run mode of the engine, data will be serialized from the binary
#file, and no deserialization will occur
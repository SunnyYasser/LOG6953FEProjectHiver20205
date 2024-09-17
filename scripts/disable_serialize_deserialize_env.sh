#!/bin/bash

export VFENGINE_DISABLE_DESERIALIZE=1
export VFENGINE_DISABLE_SERIALIZE=1
echo "VFENGINE_DISABLE_DESERIALIZE is set to: $VFENGINE_DISABLE_DESERIALIZE"
echo "VFENGINE_DISABLE_SERIALIZE is set to: $VFENGINE_DISABLE_SERIALIZE"


#This script is used to disable serializing and deserializing component of the engine
#To disable either of the feature, manually set the corresponding env variable

#!/bin/bash

source ./unset_env.sh
export VFENGINE_ENABLE_STATE_SHARING=1
echo "VFENGINE_ENABLE_STATE_SHARING is set to: $VFENGINE_ENABLE_STATE_SHARING"


#This script enables the state sharing of the engine
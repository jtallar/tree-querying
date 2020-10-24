#!/bin/bash

java -DINTERFACE_IP="$1" -cp 'lib/jars/*' "ar.edu.itba.pod.tpe.server.Server"
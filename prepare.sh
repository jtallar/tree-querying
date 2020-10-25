#!/bin/bash
mvn clean install || { echo 'mvn clean install failed' ; exit 1 ; }
cd server/target
tar -xzf tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz
chmod u+x tpe2-g6-server-1.0-SNAPSHOT/run-*
cd ../..
cd client/target
tar -xzf tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz
chmod u+x tpe2-g6-client-1.0-SNAPSHOT/run-*
chmod u+x tpe2-g6-client-1.0-SNAPSHOT/query*
cd ../..

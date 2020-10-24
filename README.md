# POD - TPE2 - G6

### Preparación
Para preparar el entorno, una vez posicionado en el directorio del proyecto, se puede ejecutar el script prepare.sh 
otorgándole permisos de ejecución (de ser necesario) y luego ejecutando el script 

`chmod u+x prepare.sh` 

`./prepare.sh` 

Esto es equivalente a ejecutar los siguientes comandos (partiendo desde una terminal en el directorio raíz del proyecto):

1. `mvn clean install`
2. `cd server/target`
3. `tar -xzf tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz`
4. `chmod u+x tpe2-g6-server-1.0-SNAPSHOT/run-*`
5. `cd ../..`
6. `cd client/target`
7. `tar -xzf tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz`
8. `chmod u+x tpe2-g6-client-1.0-SNAPSHOT/run-*`

### Server-Side
Luego, se debe levantar al menos una instancia de hazelcast, para lo cual se debe (nuevamente desde el directorio raíz del proyecto)

1. `cd server/target/tpe2-g6-server-1.0-SNAPSHOT`
2. `./run-node 'xx.xx.xx.xx'`

Donde xx.xx.xx.xx es la interfaz de red a la que se desea bindear Hazelcast. Pueden utilizarse caracteres 
de rango (* y -) para simplicidad. 
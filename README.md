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
8. `chmod u+x tpe2-g6-client-1.0-SNAPSHOT/query*`

### Server-Side
Luego, se debe levantar al menos una instancia de hazelcast, para lo cual se debe (nuevamente desde el directorio raíz del proyecto)

1. `cd server/target/tpe2-g6-server-1.0-SNAPSHOT`
2. `./run-node 'xx.xx.xx.xx'`

Donde xx.xx.xx.xx es la interfaz de red a la que se desea bindear Hazelcast. Pueden utilizarse caracteres 
de rango (* y -) para simplicidad. Por ejemplo, ` ./run-node '192.168.0.*'`

Si se desea levantar más nodos, basta con ejecutar los mismos comandos en otra terminal, indicando la misma interfaz de red.
Lógicamente, ambas terminales deben estar conectadas a dicha interfaz de red.

### Client-Side
Una vez levantado el servidor (al menos 1 nodo), para ejecutar alguna de las queries, nos movemos al directorio donde 
se encuentran los ejecutables (nuevamente desde el directorio raíz del proyecto)

`cd client/target/tpe2-g6-client-1.0-SNAPSHOT`

Y ejecutamos la query deseada.

En cada una de las queries, se debe indicar en el parámetro `-Dcity` la ciudad sobre la cual
deseamos ejecutar las consultas (BUE para Buenos Aires, VAN para Vancouver); en el parámetro
`-Daddresses`, las direcciones IP de los nodos levantados con sus puertos (una o más, separadas por ;);
en el parámetro `-DinPath` el directorio donde se encuentran los archivos arbolesXXX.csv y barriosXXX.csv 
(correspondientes a la ciudad elegida); y en el parámetro `-DoutPath` el directorio donde se desean 
guardar los archivos de salida (queryX.csv y queryX.txt).

- Para obtener el total de árboles por habitante (Query 1), se ejecuta **query1** con los parámetros 
mencionados anteriormente. Por ejemplo:

`./query1 -Dcity=VAN -Daddresses=192.168.0.10:5701 -DinPath=/home/julian/Desktop/POD/tpe2-g6/test-files/ -DoutPath=/home/julian/Desktop/POD/tpe2-g6/test-files/`

- Para obtener la calle con más árboles de cada barrio y mayores a un mínimo (Query 2), se ejecuta **query2** con los parámetros 
mencionados anteriormente y un parámetro `-Dmin`, con un valor entero mayor a cero, que será el mínimo requerido (*exclusivo*). Por ejemplo:

`./query2 -Dcity=VAN -Daddresses=192.168.0.10:5701 -DinPath=/home/julian/Desktop/POD/tpe2-g6/test-files/ -DoutPath=/home/julian/Desktop/POD/tpe2-g6/test-files/ -Dmin=300`

- Para obtener las n especies de árboles con mayor promedio de diámetro (Query 3), se ejecuta **query3** con los parámetros 
mencionados anteriormente y un parámetro `-Dn`, con un valor entero mayor a cero, que será la cantidad de especies a listar (Top n). Por ejemplo:

`./query3 -Dcity=VAN -Daddresses=192.168.0.10:5701 -DinPath=/home/julian/Desktop/POD/tpe2-g6/test-files/ -DoutPath=/home/julian/Desktop/POD/tpe2-g6/test-files/ -Dn=3`

- Para obtener los pares de barrios que contengan al menos min árboles de una especie X (Query 4), se ejecuta **query4** con los parámetros 
mencionados anteriormente, un parámetro `-Dmin`, con un valor entero mayor a cero que será el mínimo requerido (*inclusivo*), y un parámetro `-Dname`, con un 
string que será el nombre de la especie a considerar. Por ejemplo:

`./query4 -Dcity=VAN -Daddresses=192.168.0.10:5701 -DinPath=/home/julian/Desktop/POD/tpe2-g6/test-files/ -DoutPath=/home/julian/Desktop/POD/tpe2-g6/test-files/ -Dmin=11000 -Dname='Fraxinus pennsylvanica'`
 
- Para obtener los pares de árboles que registran la misma cantidad de miles de árboles (Query 5), se ejecuta **query5** con los parámetros 
mencionados anteriormente. Por ejemplo:

`./query5 -Dcity=VAN -Daddresses=192.168.0.10:5701 -DinPath=/home/julian/Desktop/POD/tpe2-g6/test-files/ -DoutPath=/home/julian/Desktop/POD/tpe2-g6/test-files/`
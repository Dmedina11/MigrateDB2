#!/usr/bin/bash

#accedemos al directorio de los csv...
cd $1
#hacemos el listado de todos los csv y los concatenamos en un unico archivo
cat $3 > total_macs.csv
#nos devolvemos a la ruta original
cd $2
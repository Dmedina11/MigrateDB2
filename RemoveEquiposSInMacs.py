#!/usr/bin/python

# NewInsertTables,py, script que permite la insercion de datos a las tablas que presentan alguna relacion entre ellas
# en la base de datos, estas son usuarios, equipos, macs y usuariosequipos, comienza con el llenado de la tabla usuario
# para luego continuar con los equipos y las macs, en general hace una migracion completa de los usuarios y equipos
# y solo asocia las macs que se encuentran en el csv de entrada, no obstante hace una eliminacion de los usuarios que
# no presentan equipos asociados y tambien de aquellos equipos que no presentan macs asociada, ademas elimina de lleno
# aquellos equipos que presentan mas de 2 macs asociadas y genera un listado con la informacion correspondiente de esos
# usuarios en formato csv, para ello se apoya del script RemoveElementsConflictiv.py
#
# Copyright (C) 30/07/2015 David Alfredo Medina Ortiz  dmedina11@alumnos.utalca.cl
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA

#modulos
import sys
import os
import psycopg2
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
                        FileTransferSpeed, FormatLabel, Percentage, \
                        ProgressBar, ReverseBar, RotatingMarker, \
                        SimpleProgress, Timer

#funcion que lee un archivo csv y obtiene la informacion de la conexion a la base de datos, retorna
#un diccionario con esta informacion, teniendo como key si es import o export dependiendo sea el caso
#y como se establezca en los input del script
#name_file = archivo con la informacion a leer
def ReadCSV (name_file):
	
	information_DB = {}#declaracion del diccionario
	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'a')#abrimos archivo log
	
	try:#manejo de errores	
		cont=0#para contar las lineas
		file_read = open(name_file)#abrimos archivo
		line = file_read.readline()#leemos primera linea
		while line:#ciclo de lectura
		
			new_line = line.replace("\n", "")#quitamos el enter
			split_line = new_line.split(';')#separamos por ;

			key=''#asigamos key segun linea
			if cont==0:
				key = 'import'
			else:
				key='export'

			information_DB[key] = split_line#asignacion a diccionario
			line = file_read.readline()
			cont+=1#aumentamos lineas
		file_read.close()
	except:#manejo de exception
		logs.write("No es posible abrir el archivo con la informacion de la conexion a la base de datos\n")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")

	logs.close()
	return information_DB#se retorna el diccionario..

#funcion que permite la conexion con la base de datos, los parametros representan:
#host => el host a conectarse, descrito por la ip
#dbname => nombre de la base de datos
#user => usuario con el cual se accedera a la base de datos
#password => clave para el usuario que se conectara a la base de datos
def ConnectDB (host, dbname, user, password):

	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'a')#abrimos archivo log
	connection = "host= %s dbname= %s user=%s password=%s" % (host, dbname, user, password)#conexion con la informacion
	var = "Connecting to database\n	->%s" % (connection)#se muestra en el log la informacion 
	logs.write(var+"\n")

	try: #manejo de try/exception
		conn = psycopg2.connect(connection)#se obtiene la conexion
		cursor_db = conn.cursor()#se recupera un cursor
		logs.write("Connected!\n")
	except:#manejo de exception
		logs.write("No es posible conectarse a la base de datos")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")
	logs.close()
	return conn, cursor_db#se retorna la conexion y el cursor de la conexion

#funcion que permite la creacion de los outputs que se generan en la ejecucion del script
def CreateFileOuputs():

	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'w')#archivo para el aviso de errores
	logs.close()

#funcion que permite crear una vsta y recupera aquellos equipos (ids) que no poseen ninguna mac asociada...
def GetEquiposSinMacs(cursor_db, connection):

	list_ids_equipos = []

	try:
		query = "drop view if exists contando_equipos"
		cursor_db.execute(query)
		connection.commit()
		
		query = "create view contando_equipos as select equipo.id as id_equipo, COUNT(*) as cantidad from equipo left outer join macs on (equipo.id = macs.iddispositivo) group by equipo.id"
		cursor_db.execute(query)
		connection.commit()


 		query = "select id_equipo from contando_equipos where id_equipo in (select id_equipo from contando_equipos where cantidad =0)"
 		cursor_db.execute(query)

 		for row in cursor_db:
 			list_ids_equipos.append(row[0])
 	except:
 		print "murio la cosa"
 		sys.exit("error en funcion GetEquiposSinMacs")
 	
 	return list_ids_equipos

def RemoveEquiposSinMacs(cursor_db, connection, list_ids_equipos):

	try:
		for id_equipo in list_ids_equipos:
			query = "DELETE FROM equipo where id = %d" % id_equipo
			cursor_db.execute(query)
			connection.commit()
	except:
 		print "murio la cosa"
 		sys.exit("error en funcion GetEquiposSinMacs")

#funcion principal...
def main ():

	CreateFileOuputs()
	# #make connections...
	data_connected_import = ConnectDB(information_DB['import'][0], information_DB['import'][1], information_DB['import'][2], information_DB['import'][3])
	data_connected_export = ConnectDB(information_DB['export'][0], information_DB['export'][1], information_DB['export'][2], information_DB['export'][3])
	print "Removiendo elementos conflictivos y elementos no existentes en csv"
	
	list_ids_equipos = GetEquiposSinMacs(data_connected_export[1], data_connected_export[0])
	RemoveEquiposSinMacs(data_connected_export[1], data_connected_export[0], list_ids_equipos)
	return 0
#llamada a la funcion principal
if __name__ == '__main__':
	main()
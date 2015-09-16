#!/usr/bin/python

# RemoveElementsConflictiv.pl, script que permite eliminar aquellos usuarios que no poseen equipos asociados
# y a aquellos equipos que no posee macs asociadas, ademas de eliminar aquellas macs que no existen en el csv
# y que no deben estar en la base de datos
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
import syslog
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
		syslog.syslog(syslog.LOG_INFO,"No es posible abrir el archivo con la informacion de la conexion a la base de datos")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return information_DB#se retorna el diccionario..

#funcion que permite la conexion con la base de datos, los parametros representan:
#host => el host a conectarse, descrito por la ip
#dbname => nombre de la base de datos
#user => usuario con el cual se accedera a la base de datos
#password => clave para el usuario que se conectara a la base de datos
def ConnectDB (host, dbname, user, password):

	connection = "host= %s dbname= %s user=%s password=%s" % (host, dbname, user, password)#conexion con la informacion
	var = "Connecting to database ->%s" % (connection)#se muestra en el log la informacion 
	syslog.syslog(syslog.LOG_INFO,var)

	try: #manejo de try/exception
		conn = psycopg2.connect(connection)#se obtiene la conexion
		cursor_db = conn.cursor()#se recupera un cursor
		syslog.syslog(syslog.LOG_INFO,"Connected!")
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible conectarse a la base de datos")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return conn, cursor_db#se retorna la conexion y el cursor de la conexion

#funcion que permite hacer la lectura y obtener todas las posibles macs a ser insertadas en la base de datos export, las cuales provienen del csv
#name_file => representa el nombre del archivo
def ReadText (name_file):

	try:
		list_macs = []#estructura de almacenamiento
		file_read = open(name_file)#abrimos archivo
		line = file_read.readline()#leemos la primera linea
		while line:#ciclo de lectura...
			new_line = line.replace('\n', '')#reemplazamos el enter y agregamos a la lista
			list_macs.append(new_line)
			line = file_read.readline()
		file_read.close()
	except:#manejo de exceptions
		syslog.syslog(syslog.LOG_INFO,"No es posible leer el archivo con la informacion de las macs")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return list_macs#se retorna la lista con las macs capturadas...

#funcion que permite remover aquellas macs ingresadas que no existen en el csv
#cursor_db => cursor de la conexion a la base de datos
#conexion_db => conexion a la base de datos
#list_macs => lista de las macs que existen en el csv
def RemoveMacsNotInCSV(cursor_db, conexion_db, list_macs):

	macs_remove = open("FilesOutput/Macs_Eliminadas.csv", 'w')#abrimos archivo de escritura para las macs eliminadas...
	list_macs_db = GetIDs(cursor_db, "mac", "macs")#obtenemos todas las macs de la base de datos...
	list_macs_exist = []#almacenara las macs que existen en la base de datos para poder hacer la actualizacion de los nombres
	#para cada una de las macs en la base de datos se busca en el csv, si no existe en csv se elimina...
	for mac_db in list_macs_db:
		existe=0#para determinar si existe
		for mac in list_macs:
			elementos = mac.split(';')
			if elementos[0] == mac_db:#comparamos, si son iguales, existe, rompemos ciclo...
				list_macs_exist.append(mac_db)
				existe=1
				break
		if existe==0:#se mantuvo, no existe en csv, se elimina...

			#obtenemos la informacion de la mac para agregarla al csv...
			query = "select * from macs where mac = '%s'"%mac_db
			cursor_db.execute(query)

			#recorremos el resultado
			for row in cursor_db:
				elementos = []
				for element in row:
					elementos.append(element)

				var = str(elementos[1])+";"+str(elementos[2])+";"+str(elementos[3])+"\n"#formamos la linea para imprimir en el csv...
				macs_remove.write(var)#escribimos en el archivo de texto...
			DeleteElement(cursor_db, conexion_db, mac_db, "macs", "mac", 1)
	macs_remove.close()
	return list_macs_exist#se retorna esta lista para poder buscarla en el csv y obtener el nombre...

#funcion que nos permite poder obtener todos los ids de los usuarios y de los equipos de la tabla usuariosequipos,
#esto con el fin de obtener los ids de los usuarios que tienen equipos asociados...
#cursor_db => representa la conexion con la base de datos 
#table => representa la tabla bajo la cual se hara la consulta...
#field => campo a obtener la informacion
def GetIDs(cursor_db, table, field):

	list_ids = []#usaremos una estructura para almacenar los datos de esta consulta, con el fin de poder trabajar con ellos posteriormente
	try:
		query = "select %s from %s" % (table, field)#consulta
		cursor_db.execute(query)
		for row in cursor_db:#obtenemos los ids, la clave correspondera
			list_ids.append(row[0])
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetIDs)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return list_ids#retornamos los ids recolectados

#funcion que permite eliminar un elemento de una tabla en particular en base a su id...
#cursor_db => cursor de la conexion de la base de datos
#connection_db => conexion a la base de datos
#id_element => id del elemento a eliminar
#table => tabla a buscar elemento para eliminar
#field => campo a comparar para generar la eliminacion
def DeleteElement(cursor_db, connection_db, id_element, table, field, option):

	try:
		if option == 0:
			query = "delete from %s where %s = %d" % (table, field, id_element)#consulta
		else:
			query = "delete from %s where %s = '%s'" % (table, field, id_element)#consulta
		cursor_db.execute(query)#ejecucion
		connection_db.commit()#actualizacion
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la eliminacion de datos en la base de datos (DeleteElement)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#funcion que elimina todos los elementos con problemas en base a la lista entregada
#cursor_db => cursor de la conexion de la base de datos
#connection_db => conexion con la base de datos
#list_ids => lista de ids de los elementos que deben ser eliminados
#table_remove => tabla a buscar para eliminar un elemento
#field_remove => campo que determina el valor a eliminar
def DeleteElementsFails(cursor_db, connection_db, list_ids, field_remove, table_remove):

	list_ids_full = GetIDs(cursor_db, field_remove, table_remove)#obtenemos la lista de ids
	#para cada id en la lista es necesario hacer la busqueda, determinar si no existe en comparacion con la otra y proceder a hacer la
	#eliminacion del elemento debido a que es un dato inservible
	for id_full in list_ids_full:
		no_existe=0#variable a cuantificar
		for id_element in list_ids:#recorremos la lista de ids...
			if id_full == id_element:#si existen rompemos ciclo y avanzamos al siguiente
				no_existe=1
				break
		if no_existe == 0:#no se encontro el id, por lo que se debe eliminar de la base de datos...
			DeleteElement(cursor_db, connection_db, id_full, table_remove, field_remove,0)

#funcion que permite actualizar el nombre a un equipo en particular dado su id...
#cursor_db_export => cursor de la conexion a la base de datos
#connection_db_export => conexion a la base de datos
#name_equipo => nuevo nombre a actualizar
#id_equipo => id del equipo que debe ser actualizado
def UpdateNameEquipo(cursor_db_export, connection_db_export, name_equipo, id_equipo):

	try:
		query = "update equipo set nombreequipo = '%s' where id = %d" % (name_equipo, id_equipo)#consulta
		cursor_db_export.execute(query)#ejecution
		connection_db_export.commit()#actualizacion
	except:#manejo de errores
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la actualizacion de nombre al equipo")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#funcion que permite obtener el id del equipo dada su mac
#cursor_db_export => cursor de la conexion a la base de datos
#mac => informacion bajo la cual se hara la busqueda
def GetIdEquipo(cursor_db, mac):

	try:
		query = "select macs.iddispositivo from macs where macs.mac = '%s'" % mac
		cursor_db.execute(query)
		for row in cursor_db:
			return row[0]
			break
	except:#manejo de errores
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#funcion que permite actualizar los nombres de los equipos segun su id...
#cursor_db_export => cursor de la conexion a la base de datos
#connexion_db => conexion a la base de datos
#list_macs_db => lista de las macs que se encuentran en la base de datos
#list_macs_csv => lista de las macs que se encuentran en el csv
def UpdateAllEquipos(cursor_db, conexion_db, list_macs_csv):

	list_macs_db = GetIDs(cursor_db, "mac", "macs")#obtenemos todas las macs de la base de datos...
	ids_equipos_ya_cambiados = []#lista de equipos con el nombre ya cambiado...
	for mac_db in list_macs_db:#recorremos la lista de macs en la base de datos y obtenemos el id del equipo al que pertenece
		id_equipo = GetIdEquipo(cursor_db, mac_db)#debemos obtener el id del equipo
		nombreequipo = ""#para almacenar el nombre del equipo
		esta=0#buscamos el id dentro de los existentes...
		for id_eq in ids_equipos_ya_cambiados:
			if id_equipo == id_eq:
				esta=1
				break
		if esta==0:#no existe, agregamos a la lista y buscamos el nombre...
			ids_equipos_ya_cambiados.append(id_equipo)
			for mac_csv in list_macs_csv:#buscamos la mac en el listado de csv...
				element = mac_csv.split(';')
				if (mac_db == element[0].upper()):
					nombreequipo=element[1].upper()
					break
			var = str(id_equipo) + ";"+nombreequipo
			name_equipo.write(var+"\n")

#funcion que recorre la base de datos y busca aquellos equipos que posean registrada mas de dos macs, retorna una lista con los equipos que
#cumplen con dicha condicion
#cursor_db => representa el cursor de la conexion 
def GetListEquiposConflictivos(cursor_db, connexion_db):

	list_equipos_conflictivos = [] #guardamos el id de estos equipos...
	
	#try:

	#query = "drop view if exists equipos_problemas"
	#cursor_db.execute(query)
	#connexion_db.commit()
	#se crea una vista...
	query = "create view equipos_problemas as select equipo.id as id_equipo, COUNT(*) as cantidad_macs from equipo join macs on (equipo.id = macs.iddispositivo) group by equipo.id"
	cursor_db.execute(query)#ejecucion de la consulta
	connexion_db.commit()#actualizacion de la base de datos
	#se hace la consulta a la vista...
	query = "select id_equipo from equipos_problemas where cantidad_macs > 2"
	cursor_db.execute(query)
	for row in cursor_db:
		list_equipos_conflictivos.append(row[0])
	#except:
	#	syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetListEquiposConflictivos)")
	#	sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return list_equipos_conflictivos

#funcion que dado un id de un equipo en particular devuelve la informacion de este, con el fin de almacenarla para realizar un
#posterior envio de informacion hacia un csv de aquellos equipos que resultan conflictivos
#cursor_db => representa el cursor de la conexion 
#id_equipo => representa el id del equipo a buscar
def GetInfoByID(cursor_db, id_equipo):

	info_byID = []#contendra la informacion por id
	
	try:
		query = "select usuario.rut, equipo.nombreequipo, tipodispositivo.tipo, usuario.fechainicio, usuario.fechatermino, macs.mac from equipo join usuariosequipos on (equipo.id = usuariosequipos.idequipo) join usuario on (usuario.id = usuariosequipos.idusuario) join tipodispositivo on (equipo.idtipodispositivo = tipodispositivo.id) join macs on (macs.iddispositivo= equipo.id) where equipo.id = %d" % id_equipo
		cursor_db.execute(query)
		for row in cursor_db:#recorremos el resultado...
			info_byID.append(row)
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetInfoByID)\n")
	return info_byID#se retorna la lista.
		
#funcion que permite recolectar la informacion para generar el csv de los equipos conflictivos...
#cursor_db => representa el cursor de la conexion 
#list_equipos_conflictivos => lista de equipos conflictivos para los cuales debe recolectar informacion
def RecolectaInformacionEquiposConflictivos(cursor_db, list_equipos_conflictivos):

	info_equipos_conflictivos = {}#diccionario para almacenar informacion
	cont=0#contador para la clave
	for equipo in list_equipos_conflictivos:#se recorren los equipos
		cont+=1#aumentamos contador
		key = str(cont)#convertimos a string para dejar como clave
		info_equipos_conflictivos[key] = GetInfoByID(cursor_db, equipo)#obtenemos la informacion de ese equipo
	return info_equipos_conflictivos

#funcion que permite recolectar la serial en base a la mac obtenida...
#cursor_db_import => representa el cursor de la conexion 
#mac => a traves de la mac se busca la informacion de la serial en la base de datos de la cual se exporta
def GetSerialByMac(cursor_db_import, mac):

	serial =""#contendra la serial
	
	try:	
		query = "select dispositivo_usuario.serial from dispositivo_usuario join macs on (dispositivo_usuario.serial = macs.serial) where macs.mac = '%s'" % mac.lower()
		cursor_db_import.execute(query)#ejecucion de la consulta
		for row in cursor_db_import:
			serial = row[0]#obtenemos la serial 
			break
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetSerialByMac)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return serial#se retorna

#funcion que recibe el diccionario de la informacion y la escribe en un csv
#cursor_db_import => representa el cursor de la conexion 
#info_dicctionary => representa el diccionario con la informacion recopilada
def GenerateCSV(info_dicctionary, cursor_db_import):

	try:	
		csv = open("FilesOutput/List_EquiposConflictivos.txt", 'w')#archivo con la informacion recopilada
		for key in info_dicctionary:#recorremos el diccionario
			line = ""#hacemos la linea
			cont=0#para determinar como se imprime
			for array in info_dicctionary[key]:#recorremos los elementos de la clave del diccionario (todos son arreglos)
				for element in array:#recorremos los elementos del arreglo
					elemento = str(element)#convertimos a string
					elemento = elemento.replace('\n', '')#quitamos el enter
					if cont != 0:#determinamos como formamos la linea
						line = line + ";"+elemento
					else:
						line = line + elemento
					cont+=1#aumentamos el contador
				serial = GetSerialByMac(cursor_db_import, array[-1])#obtenemos la serial
				line = line+";"+serial#agregamos la serial a la linea
				line = line.replace('\n', '')#reemplazamos el enter
				csv.write(line+"\n")#imprimimos en archivo	
		csv.close()	
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible generar el csv con la informacion recolectada de los equipos conflictivos")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#funcion que obtiene la lista de equipos conflictivos y los borra de la base de datos...
#cursor_db => cursor de la conexion a la base de datos
#connection => conexion a la base de datos
#list_equipos_conflictivos => lista de equipos conflictivos dispuesta en una estructura
def DeleteEquiposConflictivos(cursor_db, connection, list_equipos_conflictivos):

	try:
		for elementos in list_equipos_conflictivos:#recorremos la lista...
			query = "DELETE FROM equipo where id = %d" % elementos#hacemos la consulta y la posterior eliminacion de los datos...			
			cursor_db.execute(query)
			query = "DELETE FROM usuario where rut = '0'"#eliminamos al usuario conflictivo...
			cursor_db.execute(query)
			connection.commit()#actualizamos
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible eliminar los equipos conflictivos (DeleteEquiposConflictivos)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#obtenemos los ids de los equipos...
def GetEquipoID(cursor_db, connexion_db):

	list_equipos_id =[]

	try:
		query = "select id from equipos"
		cursor_db.execute(query)
		for row in cursor_db:
			list_equipos_id.append(row[0])
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible hacer la consulta a la base de datos (GetEquipoID)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return list_equipos_id

#se consulta por las macs asociadas a los equipos...
def GetMacOfEquipo(cursor_db, conexion, id_equipo):

	list_macs_equipos =[]

	try:
		query = "select macs.mac from equipo join macs on (macs.iddispositivo = equipo.id) where equipo.id = %d" % id_equipo
		cursor_db.execute(query)
		for row in cursor_db:
			list_macs_equipos.append(row[0])
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible hacer la consulta a la base de datos (GetMacOfEquipo)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return list_macs_equipos

#funcion principal...
def main ():

	#abrimos syslog para registrar lo que realiza el script...
	syslog.openlog("RemoveElementsConflictiv.py", syslog.LOG_USER)
	
	information_DB = ReadCSV(sys.argv[1])#obtenemos la informacion de la base de datos...
	# #make connections...
	data_connected_import = ConnectDB(information_DB['import'][0], information_DB['import'][1], information_DB['import'][2], information_DB['import'][3])
	data_connected_export = ConnectDB(information_DB['export'][0], information_DB['export'][1], information_DB['export'][2], information_DB['export'][3])
	print "Removiendo elementos conflictivos y elementos no existentes en csv"
	#obtengo la lista de macs...
	list_macs = ReadText("FilesOutput/GetMacslist_macs_not_exists.txt")
	list_macs_persistentes=RemoveMacsNotInCSV(data_connected_export[1], data_connected_export[0], list_macs)
	
	#eliminamos los datos que presentan problemas de union y que deben almacenar su informacion en un csv...
	list_equipos_conflictivos = GetListEquiposConflictivos(data_connected_export[1], data_connected_export[0])#obtenemos la lista de elementos conflictivos...
	info_equipos_conflictivos = RecolectaInformacionEquiposConflictivos(data_connected_export[1], list_equipos_conflictivos)
	GenerateCSV(info_equipos_conflictivos, data_connected_import[1])
	DeleteEquiposConflictivos(data_connected_export[1], data_connected_export[0], list_equipos_conflictivos)

	logs = open("FilesOutput/Remove.txt", 'w')
	logs.write("ok")
	logs.close()
	syslog.closelog()#cerramos syslog

	return 0
#llamada a la funcion principal
if __name__ == '__main__':
	main()
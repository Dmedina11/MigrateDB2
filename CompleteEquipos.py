#!/usr/bin/python

# CompleteEquipos.py, script que permite la insercion de todos los equipos de usuarios registrados en la base de datos,
# sin repetir elementos, ademas agrega todos los equipos de usuarios con rut no reconocible al usuario con rut 11111111,
# para su ejecucion recibe como parametro el path que contiene el archivo con la informacion a la base de datos.
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
	
#funcion que crea un diccionario con los datos y su clave es la clave primaria, hace una consulta y obtiene la informacion
#cursor_db => representa la conexion con la base de datos 
#table => representa la tabla bajo la cual se hara la consulta...
#index => la posicion en el arreglo que contiene la respuesta que posee la informacion que deseo
def MakeDictionaryTablesIndex (cursor_db, table, index):

	try:#manejo de errores
		dictionary = {}#creacion de diccionario
		query = "SELECT * FROM %s" % table#desarrollo de consulta
		cursor_db.execute(query)#se hace la consulta
		for row in cursor_db:#insercion de elementos en el diccionario, dependiendo del valor de indice---
			if index == 0:
				new_row = row[0].upper()
				dictionary[new_row] = row[1]
			else:
				new_row = row[1].upper()
				dictionary[new_row] = row[0]
	except:#manejo de errores
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (MakeDictionaryTablesIndex)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	
	return dictionary#se retorna el diccionario...

#funcion que permite buscar en el diccionario y entrega informacion sobre el valor de la clave...
#dictionary_import => diccionario que permite buscar la informacion en base a la existente en la base de datos import
#id_compare => el id a comparar para obtener el valor...
def SearchInformationInDictionary(dictionary_import, id_compare):

	key = ''
	for element in dictionary_import:#se recorre cada elemento del diccionario
		if dictionary_import[element] == id_compare:#hacemos la comparacion
			return element#retornamos el elemento si lo encontramos...
			break

#funcion que permite determinar si un elemento en particular existen en la base de datos a exportar la informacion
#data => representa la data a buscar en la base de datos
#cursor_db => representa la conexion con la base de datos 
#table => representa la tabla bajo la cual se hara la consulta...
#field => el campor a seleccionar para la consulta...
def CheckExistenceInDB(cursor_db, table, field, data):

	cont=0#para determinar si existe o no
	try:
		new_data = data.upper()#cambiamos a mayusculas
		query = ""
		if data == '':
			query = "SELECT * FROM %s where %s = NULL "% (table, field)#hacemos la consulta	
		else:
			query = "SELECT * FROM %s where %s = '%s'" % (table, field, new_data)#hacemos la consulta
		cursor_db.execute(query)#ejecutamos
		cont=0#para determinar si existe o no
		for row in cursor_db:#recorremos la lista
			cont+=1
			break
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (CheckExistenceInDB)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	
	return cont#retornamos el cont

#funcion que obtiene todos los ruts de los usuarios ingresados...
#cursor_db_export => cursor de la base de datos a la cual se posee conexion
#connection_db_export => conexion con la base de datos
def GetAllRutsUsersAdd(cursor_db_export, connection_db_export, tipo_usuario):

	ruts_usuarios_ingresados = []#estructura para almacenar los usuarios
	try:#manejo de errores
		query = "SELECT usuario.rut from usuario where usuario.idtipousuario = %d" % tipo_usuario #obtenemos la informacion del usuario
		cursor_db_export.execute(query)
		for row in cursor_db_export:
			ruts_usuarios_ingresados.append(row[0])#agregamos el rut
	except:#manejo de exceptions
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetAllRutsUsersAdd)")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: CompleteEquipos.txt")
	return ruts_usuarios_ingresados#se retornan los ruts recogidos

#funcion que permite hacer una insercion en la tabla equipo...
#cursor_db => cursor de la base de datos a la cual se tiene conexion
#connection => conexion a la base de datos
#sequence => representa una secuencia en el sql que hace al campo id autoincrementable
#id_estado => campo de la tabla equipo
#id_tipodispositivo => campo de la tabla equipo
#id_marca => campo de la tabla equipo
#nombre => campo de la tabla equipo
def InsertElementIntoEquipo(cursor_db, connection, sequence, id_estado, id_tipodispositivo, id_marca, nombre):

	insert =0#para determinar si hubo ingreso o no, representa el id del ultimo ingresado

	try:#manejo de errores
		query = "INSERT INTO equipo values (nextval('%s'),'%s', %d, %d, %d)" % (sequence, nombre, id_tipodispositivo, id_estado, id_marca)#consulta
		cursor_db.execute(query)#ejecucion
		connection.commit()#actualizacion
		#hacemos la consulta por el que se acaba de ingresar para obtener el id...
		query = "SELECT equipo.id from equipo where nombreequipo = '%s' and idtipodispositivo = %d and idestado = %d and idmarca = %d" % (nombre, id_tipodispositivo, id_estado, id_marca)
		cursor_db.execute(query)
		for row in cursor_db:
			insert=row[0]
			break
	except:#manejo de exceptions
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la insercion de datos en la tabla equipo (InsertElementIntoEquipo)")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: CompleteEquipos.txt")

	return insert

#funcion que permite insertar elementos en la base de datos en la tabla usuariosequipos
#cursor_db => cursor de la base de datos a la cual se tiene conexion
#connection => conexion a la base de datos
#id_equipo => campo de la tabla usuariosequipos
#id_usuario => campo de la tabla usuariosequipos
#sequence => representa una secuencia en el sql que hace al campo id autoincrementable
def InsertIntoUsuariosEquipos(cursor_db, connection_db, id_equipo, id_usuario, sequence):

	try:#manejo de errores
		query = "INSERT INTO usuariosequipos VALUES (nextval('%s'), %d, %d)" % (sequence, id_equipo, id_usuario)#hacemos la insercion de datos
		cursor_db.execute(query)
		connection_db.commit()	
	except:#manejo de exceptions
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la insercion de datos en la tabla usuariosequipos (InsertIntoUsuariosEquipos)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#funcion que permite poder obtener el id de un registro en particular...
#cursor_db => cursor de la base de datos a la cual se tiene conexion
#table => tabla a la que se hara la consulta
#field => campo seleccionador
#element => elemento a comparar
#index => posicion en respuesta de la consulta
def GetIDInTable(cursor_db, table, field, element, index):

	id_element=""#almacenara el id del elemento

	try:#manejo de errores
		query = "SELECT * FROM %s where %s = '%s'" %(table, field, element)#consulta
		cursor_db.execute(query)#ejecucion
		for row in cursor_db:#recorremos
			id_element=row[index]#obtenemos el id
			break
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetIDInTable)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return id_element#se retorna el id del elemento a buscar...

#funcion que permite obtener todos los equipos asociados a los usuarios
#cursor_db => cursor de la base de datos a la cual se tiene conexion
def GetEquiposOfUsers(cursor_db, id_tipo_us):

	information_equipos = []#almacenamos la informacion de los equipos en una estructura
	
	try:#manejo de errores
		query = "select usuario.rut, dispositivo_usuario.serial, dispositivo_usuario.id_estado, dispositivo_usuario.id_marca, dispositivo_usuario.id_tipo_disp from usuario join dispositivo_usuario on (usuario.rut = dispositivo_usuario.rut) where usuario.id_tipo_us = %d" % id_tipo_us #consulta
		cursor_db.execute(query)#ejecucion
		for row in cursor_db:#se recorre la respuesta y almacena la informacion en la estructura
			information_equipos.append(row)
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetEquiposOfUsers)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return information_equipos#se retorna la informacion recolectada

#funcion que permite el ingreso de elementos a la tabla equipo, ademas autocompleta la tabla usuariosequipos
#cursor_db_import => cursor de la conexion a la base de datos import
#cursor_db_export => cursor de la conexion a la base de datos export
#connection_db_export => conexion a la base de datos export
#state_device_import => diccionario con la informacion del estado del dispositivo en la base de datos import
#state_device_export => diccionario con la informacion del estado del dispositivo en la base de datos export
#kind_device_import => diccionario con la informacion del tipo del dispositivo en la base de datos import
#kind_device_export => diccionario con la informacion del estado del dispositivo en la base de datos export
#trademark_import => diccionario con la informacion de la marca en la base de datos import
#trademark_export => diccionario con la informacion de la marca en la base de datos export
def InsertEquipos(cursor_db_import, cursor_db_export, connection_db_export, state_device_import, state_device_export, kind_device_import, kind_device_export, trademark_import, trademark_export, tipo_usuario, id_tipo_us):

	ruts_usuarios_ingresados = GetAllRutsUsersAdd(cursor_db_export, connection_db_export, tipo_usuario)#obtenemos los usuarios ingresados...
	information_equipos = GetEquiposOfUsers(cursor_db_import, id_tipo_us)#obtenemos la informacion de los usuarios y sus equipos
	syslog.syslog(syslog.LOG_INFO,"Insert elements into table equipo and auto completing elements into table usuariosequipos")
	widgets = ['Progreso: ', Percentage(), ' ', Bar(marker=RotatingMarker()), ' ', ETA(), ' ', FileTransferSpeed()]#barra de progreso
	max_value = len(information_equipos)*len(ruts_usuarios_ingresados)#maximo valor
	pbar = ProgressBar(widgets=widgets, maxval=max_value).start()#inicio de la barra de progreso
	i=0	#para ir actualizando la barra de 
	for rut in ruts_usuarios_ingresados:#recorremos todos los ruts ingresados...
		for element in information_equipos:#recorremos la informacion generica...
	 		pbar.update(i+1)#actualizacion de la barra de progreso...
	 		if str(rut) == str(element[0]):#es un equipo que corresponde al usuario con rut seleccionado...
	 			#obtenemos la informacion del equipo...
	 			key_estado_dispositivo= SearchInformationInDictionary(state_device_import, element[2])
	 			id_estado_dispositivo = state_device_export[key_estado_dispositivo]#get idestado, field table equipo
	 			id_marca = trademark_export[SearchInformationInDictionary(trademark_import, element[3])]#get idmarca, field table equipo
	 			id_tipodispositivo = kind_device_export[SearchInformationInDictionary(kind_device_import, element[4])]#get idtipodispositivo, field table equipo
	 			#insertamos los datos en la base de datos en la tabla equipo
	 			id_last_equipo_insertado =InsertElementIntoEquipo(cursor_db_export, connection_db_export, 'equipo_id_seq', id_estado_dispositivo, id_tipodispositivo, id_marca, element[1])
	 			if id_last_equipo_insertado !=0:#quiere decir que se agrego un equipo, por lo que se hace la consulta por el id del usuario y el id del equipo ingresado...
	 				#obtenemos el rut del usuario al equipo asociado...
	 				id_usuario = GetIDInTable(cursor_db_export, "usuario", "rut", rut, 0)
	 				#hacemos la insercion en la tabla usuariosequipos...
	 				InsertIntoUsuariosEquipos(cursor_db_export, connection_db_export, id_last_equipo_insertado, id_usuario, "usuariosequipos_id_seq")
	 		i+=1

#funcion que permite poder obtener el id del tipo de usuario en base al tipo que posee..
def GetTipoUsuario(cursor_db_export, tipo_usuario):

	query = "select id from tipousuario where tipo = '%s'" % tipo_usuario
	cursor_db_export.execute(query)
	for element in cursor_db_export:
		return element[0]

#funcion que permite agregar los equipos de todos los usuarios con rut irreconocible al usuario con rut 11111111, es decir
#al 00000000, 22222222
def CompleteEquiposFaltantes(cursor_db_export, connection_db_export, cursor_db_import,state_device_import, state_device_export, kind_device_import, kind_device_export, trademark_import, trademark_export):

	id_rut =0
	
	try:
		query = "select id from usuario where usuario.rut = 11111111"#obtenemos el id del usuario...
		cursor_db_export.execute(query)
		for row in cursor_db_export:
			id_rut=row[0]

		#ahora obtenemos los equipos de los ruts raros, junto con la informacion correspondiente...
		query = "select serial, id_estado, id_marca, id_tipo_disp from dispositivo_usuario where rut = '22222222' or rut = '0' or rut = '00000000' or rut = ''"
		cursor_db_import.execute(query)
		
		#obtenemos toda la informacion de todos los equipos que se requieren insertar...
		for row in cursor_db_import:
			element_equipos = []
			for element in row:
				element_equipos.append(element)
			#para cada equipo obtenemos la informacion que interesa...
			key_estado_dispositivo= SearchInformationInDictionary(state_device_import, element_equipos[1])
			id_estado_dispositivo = state_device_export[key_estado_dispositivo]#get idestado, field table equipo
			id_marca = trademark_export[SearchInformationInDictionary(trademark_import, element_equipos[2])]#get idmarca, field table equipo
			id_tipodispositivo = kind_device_export[SearchInformationInDictionary(kind_device_import, element_equipos[3])]#get idtipodispositivo, field table equipo
			#insertamos los datos en la base de datos en la tabla equipo
			id_last_equipo_insertado =InsertElementIntoEquipo(cursor_db_export, connection_db_export, 'equipo_id_seq', id_estado_dispositivo, id_tipodispositivo, id_marca, element_equipos[0])
			if id_last_equipo_insertado !=0:#quiere decir que se agrego un equipo, por lo que se hace la consulta por el id del usuario y el id del equipo ingresado...
				#hacemos la insercion en la tabla usuariosequipos...
				InsertIntoUsuariosEquipos(cursor_db_export, connection_db_export, id_last_equipo_insertado, id_rut, "usuariosequipos_id_seq")
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetEquiposOfUsers)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#funcion principal				
def main ():

	#abrimos syslog para registrar lo que realiza el script...
	syslog.openlog("CompleteEquipos.py", syslog.LOG_USER)

	information_DB = ReadCSV(sys.argv[1])#obtenemos la informacion de la base de datos...
	data_connected_import = ConnectDB(information_DB['import'][0], information_DB['import'][1], information_DB['import'][2], information_DB['import'][3])#conexion db import
	data_connected_export = ConnectDB(information_DB['export'][0], information_DB['export'][1], information_DB['export'][2], information_DB['export'][3])#conexion db export
	#creacion de los diccionarios
	kind_user_import = MakeDictionaryTablesIndex(data_connected_import[1], "tipo_usuario", 0)
	kind_user_export = MakeDictionaryTablesIndex(data_connected_export[1], "tipousuario", 1)	
	kind_device_import = MakeDictionaryTablesIndex(data_connected_import[1], "tipo_dispositivo", 1)
	kind_device_export = MakeDictionaryTablesIndex(data_connected_export[1], "tipodispositivo", 1)
	state_device_import = MakeDictionaryTablesIndex(data_connected_import[1], "estado", 1)
	state_device_export = MakeDictionaryTablesIndex(data_connected_export[1], "estadodispositivo", 1)
	trademark_import = MakeDictionaryTablesIndex(data_connected_import[1], "marca", 1)
	trademark_export = MakeDictionaryTablesIndex(data_connected_export[1], "marca", 1)
	estado_usuario_export = MakeDictionaryTablesIndex(data_connected_export[1], "estadousuario", 1)
	
	print "insertando equipos Alumnos"
	syslog.syslog(syslog.LOG_INFO,"insertando equipos Alumnos")
	id_tipo = GetTipoUsuario(data_connected_export[1], "ALUMNO")
	InsertEquipos(data_connected_import[1], data_connected_export[1], data_connected_export[0] , state_device_import, state_device_export, kind_device_import, kind_device_export, trademark_import, trademark_export, id_tipo, 1)
	print "insertando equipos Docentes"
	syslog.syslog(syslog.LOG_INFO,"insertando equipos Docentes")
	id_tipo = GetTipoUsuario(data_connected_export[1], "DOCENTE")
	InsertEquipos(data_connected_import[1], data_connected_export[1], data_connected_export[0] , state_device_import, state_device_export, kind_device_import, kind_device_export, trademark_import, trademark_export, id_tipo, 2)
	print "insertando equipos Administrativos"
	syslog.syslog(syslog.LOG_INFO,"insertando equipos Administrativos")
	id_tipo = GetTipoUsuario(data_connected_export[1], "ADMINISTRATIVO")
	InsertEquipos(data_connected_import[1], data_connected_export[1], data_connected_export[0] , state_device_import, state_device_export, kind_device_import, kind_device_export, trademark_import, trademark_export, id_tipo, 3)
	print "insertando equipos Visitas"
	syslog.syslog(syslog.LOG_INFO,"insertando equipos Visitas")
	id_tipo = GetTipoUsuario(data_connected_export[1], "VISITA")
	InsertEquipos(data_connected_import[1], data_connected_export[1], data_connected_export[0] , state_device_import, state_device_export, kind_device_import, kind_device_export, trademark_import, trademark_export, id_tipo, 4)
	print "insertando equipos Transicion"
	syslog.syslog(syslog.LOG_INFO,"insertando equipos Transicion")
	id_tipo = GetTipoUsuario(data_connected_export[1], "TRANSICION")
	InsertEquipos(data_connected_import[1], data_connected_export[1], data_connected_export[0] , state_device_import, state_device_export, kind_device_import, kind_device_export, trademark_import, trademark_export, id_tipo, 5)
	print "insertando equipos Alumnos Intercambio"
	syslog.syslog(syslog.LOG_INFO,"insertando equipos Alumnos Intercambio")
	id_tipo = GetTipoUsuario(data_connected_export[1], "ALUMNO INTERCAMBIO")
	InsertEquipos(data_connected_import[1], data_connected_export[1], data_connected_export[0] , state_device_import, state_device_export, kind_device_import, kind_device_export, trademark_import, trademark_export, id_tipo, 6)
	
	CompleteEquiposFaltantes(data_connected_export[1], data_connected_export[0], data_connected_import[1],state_device_import, state_device_export, kind_device_import, kind_device_export, trademark_import, trademark_export)
	
	logs = open("FilesOutput/CompleteEquipos.txt", 'w')#abrimos archivo log
	logs.write("ok")
	logs.close()
	syslog.closelog()#cerramos syslog
	return 0

#llamada a la funcion principal...
if __name__ == '__main__':
	main()
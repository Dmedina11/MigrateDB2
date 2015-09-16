#!/usr/bin/python

# CompleteUsuarios.py, es un script que permite la insercion de todos los usuarios con equipos asociados en la nuevea
# base de datos, ademas gestiona aquellos usuarios que no poseen un equipo asociado y los deposita en un archivo csv
# con la informacion correspondiente, por otro lado no considera la insercion de aquellos usuarios con rut irreconocible
# y solo permite al usuario 11111111, no obstante aquellos usuarios que presentan doble rol, estudiante-docente o 
# docente-administrativo se le privilegia el rol de administrativo, por otro lado se trabaja tambien con aquellos
# usuarios que presentan doble fecha de ingreso, dejando la mas reciente.
# Script recibe como parametro de entrada el path que contiene el archivo con la informacion de las conexiones 
# a las bases de datos.
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
	var = "Connecting to database\n	->%s" % (connection)#se muestra en el log la informacion 
	syslog.syslog(syslog.LOG_INFO,var)

	try: #manejo de try/exception
		conn = psycopg2.connect(connection)#se obtiene la conexion
		cursor_db = conn.cursor()#se recupera un cursor
		syslog.syslog(syslog.LOG_INFO,"Connected!")
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible conectarse a la base de datos")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

	return conn, cursor_db#se retorna la conexion y el cursor de la conexion

#funcion que permite la creacion de los outputs que se generan en la ejecucion del script
def CreateFileOuputs():

	outputs_usuarios = open("FilesOutput/List_users_Delete.txt", 'w')#archivo para el registro de usuarios no ingresados en la base de datos y/o eliminados por diversas causas
	outputs_usuarios.close()

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

#funcion que permite poder obtener toda la informacion de los usuarios que tienen asociados equipos y estos macs, con el fin de poder hacer la
#insercion de estos usuarios en la base de datos...
#cursor_db => representa la conexion con la base de datos 
def GetAllInformationofDataBase(cursor_db, tipo_usuario):

	information_generic = []#estrunctura para almacenar la informacion de la base de datos en base a la consulta que se realice
	try:#manejo de errores
		query = "select usuario.rut, usuario.id_tipo_us, usuario.fecha_fin, usuario.fecha_ini, dispositivo_usuario.serial, dispositivo_usuario.id_estado, dispositivo_usuario.id_marca, dispositivo_usuario.id_tipo_disp, macs.mac, macs.id_tipo_int from usuario join dispositivo_usuario on (dispositivo_usuario.rut = usuario.rut) join macs on (macs.serial = dispositivo_usuario.serial) where usuario.id_tipo_us = %d" % tipo_usuario
		cursor_db.execute(query)#ejecutamos la consulta..
		for row in cursor_db:#agregamos a una lista...
			information_generic.append(row)
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetAllInformationofDataBase)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	
	return information_generic#se retorna la lista con la informacion recopilada

#funcion que permite recibir el estado del dispositivo y en base a ello completa el estado del usuario
#cursor_db => representa la conexion con la base de datos 
#id_estadodispositivo => representa el id del estado del dispositivo por el cual se va a consultar
def GetIDEstadoUsuario(cursor_db, id_estadodispositivo):

	try:#manejo de errores	
		query = "SELECT nombre FROM estadodispositivo WHERE id = "+ str(id_estadodispositivo)#consulta
		cursor_db.execute(query)#ejecucion
		nombre = ""#para almacenar el nombre...
		for row in cursor_db:#obtenemos el campo
			nombre = row
			break

		query = "SELECT id FROM estadousuario WHERE nombre = '%s'" % nombre#se crea una nueva consulta con la informacion obtenida
		cursor_db.execute(query)#se ejecuta
		id_estadousuario=""#para obtener el id
		
		for row in cursor_db:#obtenemos el campo que interesa
			id_estadousuario = row[0]#lo asignamos...
			break
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetIDEstadoUsuario)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return id_estadousuario#retornamos el id_estadousuario

#funcion que permite determinar si un elemento en particular existen en la base de datos a exportar la informacion
#data => representa la data a buscar en la base de datos
#cursor_db => representa la conexion con la base de datos 
#table => representa la tabla bajo la cual se hara la consulta...
#field => el campor a seleccionar para la consulta...
def CheckExistenceInDBUser(cursor_db, table, field, data, field2, data2, field3, data3):

	cont=0#para determinar si existe o no
	try:
		new_data = data.upper()#cambiamos a mayusculas
		query = ""
		if data == '':
			query = "SELECT * FROM %s where %s = NULL and %s = %d and %s = '%s'"% (table, field, field2, data2, field3, data3)#hacemos la consulta	
		else:
			query = "SELECT * FROM %s where %s = '%s' and %s = %d and %s = '%s'" % (table, field, new_data, field2, data2, field3, data3)#hacemos la consulta
		cursor_db.execute(query)#ejecutamos
		cont=0#para determinar si existe o no
		for row in cursor_db:#recorremos la lista
			cont+=1
			break
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (CheckExistenceInDB)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return cont#retornamos el cont

#funcion que permite hacer la insercion de datos en la tabla usuario, si y solo si el usuario ya no exista
#cursor_db => representa el cursor de la conexion con la base de datos 
#connection => representa la conexion con la base de datos 
#sequence => representa el nombre de una secuencia declara en el sql de la base de datos y que hace autoincrementable la PK
#fecha_ini => campo de la tabla usuario
#fecha_fin => campo de la tabla usuario
#rut => campo de la tabla usuario
#tipo_usuario => campo de la tabla usuario
#tipo_estado_usuario => campo de la tabla usuario
def InsertElementInUsuario(cursor_db, connection, sequence, fecha_ini, fecha_fin, rut, tipo_usuario, tipo_estado_usuario):

	try:#manejo de errores
		if (CheckExistenceInDBUser(cursor_db, 'usuario', 'rut',rut, 'idtipousuario', tipo_usuario, 'fechainicio', fecha_ini)==0):#revisamos si existe el usuario
			
			if rut == '':
				query =	"INSERT INTO usuario values (nextval('%s'),'%s', '%s', -1, %d, %d)" % (sequence, fecha_ini, fecha_fin, tipo_usuario, tipo_estado_usuario)#hacemos la consulta de insercion
			else:
				query = "INSERT INTO usuario values (nextval('%s'),'%s', '%s', '%s', %d, %d)" % (sequence, fecha_ini, fecha_fin, rut, tipo_usuario, tipo_estado_usuario)#hacemos la consulta de insercion				
			cursor_db.execute(query)#ejecutamos
			connection.commit()#actualizamos el estado de la base de datos			
		else:#si ya existe se escribe en el log
			var =  "user whit rut %s exists in table usuario" % rut
			syslog.syslog(syslog.LOG_INFO,var)
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la insercion de datos en la tabla usuario (InsertElementInUsuario)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#funcion que permite obtener el tipo de usuario...
def GetTipoUsuario(cursor_db, tipo_usuario_import):

	tipo = ""

	if tipo_usuario_import == 1:
		tipo = "ALUMNO"
	if tipo_usuario_import == 2:
		tipo = "DOCENTE"
	if tipo_usuario_import == 3:
		tipo = "ADMINISTRATIVO"
	if tipo_usuario_import == 4:
		tipo = "VISITA"
	if tipo_usuario_import == 5:
		tipo = "TRANSICION"
	if tipo_usuario_import == 6:
		tipo = "ALUMNO INTERCAMBIO"

	query = "select id from tipousuario where tipo = '%s'" % tipo
	cursor_db.execute(query)

	for row in cursor_db:
		return row[0]
		break

#funcion que permite la insercion de usuarios dentro de la base de datos...
#cursor_db_export => representa el cursor de la conexion con la base de datos 
#connection_db_export => representa la conexion con la base de datos 
#information_generic => posee la informacion de la consulta realizada
#kind_user_import => diccionario de tipo de usuario con los valores de import
#kind_user_export => diccionario de tipo de usuario con los valores de export
#state_device_import => diccionario del estado dispositivo con los valores de import
#state_device_export => diccionario de tipo de usuario con los valores de export
def InsertUsuarios(cursor_db_export, connection_db_export, information_generic, kind_user_import, kind_user_export, state_device_import, state_device_export, cursor_db_import):

	syslog.syslog(syslog.LOG_INFO,"Insert elements into table usuario")
	widgets = ['Progreso: ', Percentage(), ' ', Bar(marker=RotatingMarker()), ' ', ETA(), ' ', FileTransferSpeed()]#barra de progreso
	pbar = ProgressBar(widgets=widgets, maxval=len(information_generic)).start()#se inicia la barra
	i=0	#para actualizar la barra
	for element in information_generic:#se recorre cada elemento en la informacion generica...
		pbar.update(i+1)#actualizamos
		#obtenemos los datos que interesan del usuario...
		rut_user = str(element[0])
		fecha_ini = str(element[3])
		fecha_fin = str(element[2])
		#preguntamos por los valores de los usuarios...
		if rut_user == '0':
			rut_user == '44444444'
		id_tipousuario = GetTipoUsuario(cursor_db_export, element[1])
		key_estado_dispositivo= SearchInformationInDictionary(state_device_import, element[5])
		id_estado_dispositivo = state_device_export[key_estado_dispositivo]#get idestado, field table equipo
		id_estadousuario = GetIDEstadoUsuario(cursor_db_export, id_estado_dispositivo)#get id_estadousuario, field of table usuario
		#tenemos todos los datos, es posible realizar la insercion de usuarios...
		InsertElementInUsuario(cursor_db_export, connection_db_export, 'usuario_id_seq', fecha_ini, fecha_fin, rut_user, id_tipousuario, id_estadousuario)
		i+=1
	#para actualizar al usuario con rut 00000000
	query = "update usuario set rut = 44444444 where rut = 0"
	cursor_db_export.execute(query)
	connection_db_export.commit()	
	pbar.finish()

def InsertUsuariosFaltantes(cursor_db_export, connection_db_export, cursor_db_import, kind_user_import, kind_user_export, id_tipo_us):
	
	outputs_usuarios = open("FilesOutput/List_users_Delete.txt", 'a')#archivo para el registro de usuarios no ingresados en la base de datos y/o eliminados por diversas causasoutputs_usuarios = open()
	try:#manejo de errores

		#hacemos un query para obtener el estado de usuario deshabilitado
		query = "select * from estadousuario where descripcion = 'DESACTIVADO OTRA CAUSA'"
		cursor_db_export.execute(query)
		estado=0
		for row in cursor_db_export:
			estado = row[0]
			break

		#ahora hacemos la consulta por aquellos usuarios que no se ingresaron y que no poseen ningun equipo asociado...
		query = "select rut from usuario where rut not in (select usuario.rut from usuario join dispositivo_usuario on (dispositivo_usuario.rut = usuario.rut) join macs on (macs.serial = dispositivo_usuario.serial)) and usuario.id_tipo_us = %d" % id_tipo_us
		cursor_db_import.execute(query)
		ruts_faltantes = []#declaramos una lista de los ruts sin equipos asociados...
		for row in cursor_db_import:
			ruts_faltantes.append(row[0])

		widgets = ['Progreso: ', Percentage(), ' ', Bar(marker=RotatingMarker()), ' ', ETA(), ' ', FileTransferSpeed()]#barra de progreso
		pbar = ProgressBar(widgets=widgets, maxval=len(ruts_faltantes)).start()#se inicia la barra
		i=0	#para actualizar la barra
	 	#para cada rut en la lista buscamos la informacion y la insertamos en la base de datos...
		for rut in ruts_faltantes:

			#hacemos un select de la informacion...
			query = "select usuario.id_tipo_us, usuario.fecha_ini, usuario.fecha_fin from usuario where rut = '%s'" % rut
	 		cursor_db_import.execute(query)

	 		for row in cursor_db_import:#obtenemos la informacion para hacer la insercion de los datos...
	 			id_tipousuario = GetTipoUsuario(cursor_db_export, id_tipo_us)
	 			fecha_ini = str(row[1])
	 			fecha_fin = str(row[2])
	 			#se escribe la informacion del usuario dentro del archivo...
	 			user_insert = str(rut)+";"+fecha_ini+";"+fecha_fin+";"+str(id_tipousuario)+";"+str(estado)+"\n"
	 			outputs_usuarios.write(user_insert)
	 			#InsertElementInUsuario(cursor_db_export, connection_db_export, 'usuario_id_seq', fecha_ini, fecha_fin, rut, id_tipousuario, estado)	
	 			break
		 	pbar.update(i+1)
		 	i+=1
		pbar.finish()
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la insercion de datos en la tabla usuario (InsertElementInUsuario)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	outputs_usuarios.close()

#funcion que obtiene todos los ruts de los usuarios ingresados...
#cursor_db_export => cursor de la base de datos a la cual se posee conexion
#connection_db_export => conexion con la base de datos
def GetAllRutsUsersAdd(cursor_db_export, connection_db_export):

	ruts_usuarios_ingresados = []#estructura para almacenar los usuarios
	try:#manejo de errores
		query = "SELECT usuario.rut from usuario"#obtenemos la informacion del usuario
		cursor_db_export.execute(query)
		for row in cursor_db_export:
			ruts_usuarios_ingresados.append(row[0])#agregamos el rut
	except:#manejo de exceptions
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetAllRutsUsersAdd)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	
	return ruts_usuarios_ingresados#se retornan los ruts recogidos

#funcion que permite escribir en un archivo de texto
def Escribir(ruts, name_file):

	file_read = open(name_file, "w")

	for rut in ruts:
		var = str (rut) + "\n"
		file_read.write(var)
	file_read.close()			

#funcion que permite completar los casos especiales, tales como el 0 o el 00000000
def RemueveUsuariosComplicados(cursor_db_export, connection_db_export):

	outputs_usuarios = open("FilesOutput/List_users_Delete.txt", 'a')#archivo para el registro de usuarios no ingresados en la base de datos y/o eliminados por diversas causasoutputs_usuarios = open()
	try:
		query = "delete from usuario where rut = 44444444"
		query1 = "select rut, fechainicio, fechatermino, idtipousuario, idestadousuario from usuario where rut = 44444444"
		query2 = "delete from usuario where rut = 22222222"
		query22 = "select rut, fechainicio, fechatermino, idtipousuario, idestadousuario from usuario where rut = 22222222"
		cursor_db_export.execute(query1)
		list_elements = []

		for row in cursor_db_export:
			for element in row:
				list_elements.append(element)	
	
		var = str(list_elements[0])+";"+str(list_elements[1])+";"+str(list_elements[2])+";"+str(list_elements[3])+"\n"
		outputs_usuarios.write(var)
		cursor_db_export.execute(query22)
		list_elements = []

		for row in cursor_db_export:
			for element in row:
				list_elements.append(element)	

		var = str(list_elements[0])+";"+str(list_elements[1])+";"+str(list_elements[2])+";"+str(list_elements[3])+"\n"
		outputs_usuarios.write(var)
		cursor_db_export.execute(query)
		cursor_db_export.execute(query2)
		connection_db_export.commit()
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetAllRutsUsersAdd)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	outputs_usuarios.close()

#funcion que permite eliminar los usuarios dobles, ya sea con doble rol o con doble fecha...
def RemoverUsuariosDobles(cursor_db_export, connection_db_export):

	outputs_usuarios = open("FilesOutput/List_users_Delete.txt", 'a')#archivo para el registro de usuarios no ingresados en la base de datos y/o eliminados por diversas causasoutputs_usuarios = open()
	
	#borramos la vista en el caso de que exista...
	#query = "drop view if exists vista_usuarios_count"
	#cursor_db_export.execute(query)
	#connection_db_export.commit()

	#se crea una vista para realizar la consulta
	query = "create view vista_usuarios_count as select  usuario.rut as rut_users, usuario.idtipousuario as id_tipo, COUNT(*) as cantidad from usuario group by usuario.rut, usuario.idtipousuario"#consulta para la vista
	cursor_db_export.execute(query)#ejecutamos la consulta
	connection_db_export.commit()#hacemos un commit para actualizar la base de datos

	query = "select rut_users from vista_usuarios_count where cantidad >1  and id_tipo in (select id from tipousuario where tipo != 'ALUMNO' or tipo != 'ALUMNO INTERCAMBIO')"#hacemos una consulta sobre la vista creada para obtener aquellos usuarios con mas de un rol
	cursor_db_export.execute(query)#ejecutamos la consulta

	list_ruts = []#almacenamos los usuarios...

	for row in cursor_db_export:
		list_ruts.append(row[0])

	#por cada rut obtenemos la informacion de el, en este caso el id 
	for ruts in list_ruts:
		query = "select id from usuario where rut = %d" %ruts
		ids = []
		cursor_db_export.execute(query)

		for row in cursor_db_export:#obtenemos todos los ids...
			ids.append(row[0])

		if len(ids)>1:#si el largo del id es mayor a 1....
			#tomamos el primer elemento y hacemos un delete en la base de datos
			#obtenemos la informacion...
			query = "select rut, fechainicio, fechatermino, idtipousuario, idestadousuario from usuario where id = %d"%ids[0]
			information = []
			cursor_db_export.execute(query)

			for row  in cursor_db_export:
				for element in row:
					information.append(element)
			var = str(information[0])+";"+str(information[1])+";"+str(information[2])+";"+str(information[3])+"\n"
			outputs_usuarios.write(var)
			query = "delete from usuario where id = %d" %ids[0]
			cursor_db_export.execute(query)
			connection_db_export.commit()
	outputs_usuarios.close()
#funcion principal				
def main ():

	#abrimos syslog para registrar lo que realiza el script...
	syslog.openlog("CompleteUsuarios.py", syslog.LOG_USER)

	CreateFileOuputs()#creamos los archivos de salida
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
	
	print "Insertando Alumnos"
	syslog.syslog(syslog.LOG_INFO,"Insertando Alumnos")
	information_generic = GetAllInformationofDataBase(data_connected_import[1], 1)
	InsertUsuarios(data_connected_export[1], data_connected_export[0], information_generic, kind_user_import, kind_user_export, state_device_import, state_device_export, data_connected_import[1])
	InsertUsuariosFaltantes(data_connected_export[1], data_connected_export[0], data_connected_import[1], kind_user_import, kind_user_export, 1)
	
	print "Insertando Docentes"
	syslog.syslog(syslog.LOG_INFO,"Insertando Docentes")
	information_generic = GetAllInformationofDataBase(data_connected_import[1], 2)
	InsertUsuarios(data_connected_export[1], data_connected_export[0], information_generic, kind_user_import, kind_user_export, state_device_import, state_device_export, data_connected_import[1])
	InsertUsuariosFaltantes(data_connected_export[1], data_connected_export[0], data_connected_import[1], kind_user_import, kind_user_export, 2)
	
	print "Insertando Administrativos"
	syslog.syslog(syslog.LOG_INFO,"Insertando Administrativos")
	information_generic = GetAllInformationofDataBase(data_connected_import[1], 3)
	InsertUsuarios(data_connected_export[1], data_connected_export[0], information_generic, kind_user_import, kind_user_export, state_device_import, state_device_export, data_connected_import[1])
	InsertUsuariosFaltantes(data_connected_export[1], data_connected_export[0], data_connected_import[1], kind_user_import, kind_user_export, 3)

	print "Insertando Visitas"
	syslog.syslog(syslog.LOG_INFO,"Insertando Visitas")
	information_generic = GetAllInformationofDataBase(data_connected_import[1], 4)
	InsertUsuarios(data_connected_export[1], data_connected_export[0], information_generic, kind_user_import, kind_user_export, state_device_import, state_device_export, data_connected_import[1])
	InsertUsuariosFaltantes(data_connected_export[1], data_connected_export[0], data_connected_import[1], kind_user_import, kind_user_export, 4)

	print "Insertando Transicion"
	syslog.syslog(syslog.LOG_INFO,"Insertando Transicion")
	information_generic = GetAllInformationofDataBase(data_connected_import[1], 5)
	InsertUsuarios(data_connected_export[1], data_connected_export[0], information_generic, kind_user_import, kind_user_export, state_device_import, state_device_export, data_connected_import[1])
	InsertUsuariosFaltantes(data_connected_export[1], data_connected_export[0], data_connected_import[1], kind_user_import, kind_user_export, 5)

	print "Insertando Alumnos Intercambio"
	syslog.syslog(syslog.LOG_INFO,"Insertando Alumnos Intercambio")
	information_generic = GetAllInformationofDataBase(data_connected_import[1], 6)
	InsertUsuarios(data_connected_export[1], data_connected_export[0], information_generic, kind_user_import, kind_user_export, state_device_import, state_device_export, data_connected_import[1])
	InsertUsuariosFaltantes(data_connected_export[1], data_connected_export[0], data_connected_import[1], kind_user_import, kind_user_export, 6)	

	RemueveUsuariosComplicados(data_connected_export[1], data_connected_export[0])
	RemoverUsuariosDobles(data_connected_export[1], data_connected_export[0])
	
	logs = open("FilesOutput/CompleteUsuarios.txt", 'w')#archivo log
	logs.write("ok")
	logs.close()
	syslog.closelog()#cerramos syslog
	return 0

#llamada a la funcion principal...
if __name__ == '__main__':
	main()
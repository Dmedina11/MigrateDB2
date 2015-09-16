#!/usr/bin/python

# CompleteTables.py, script que recolecta la informacion de las tablas que no poseen una 
# dependencia en la base de datos y hace la copia de informacion de ellas en la base de datos 
# a exportar la informacion, ademas en base a las condiciones solicitadas, toda la informacion
# que se ingresa a la base de datos debe es ingresada en mayusculas...
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

#modulos a utilziar
import sys
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
	
	information_DB = {}#diccionario que contendra la informacion de los datos de las conexiones

	#manejo try/except de errores
	try:	
		cont=0#para determinar el numero de linea que se esta leyendo
		file_read = open(name_file)#abrimos archivo 
		line = file_read.readline()#leemos la primera linea
		while line:#ciclo de lectura del archivo
		
			new_line = line.replace("\n", "")#reemplazamos el enter si es que posee...
			split_line = new_line.split(';')#la informacion viene en formato csv, por lo que se separa por el ;
			key=''#determinamos la key en base al numero de linea que se este leyendo
			if cont==0:
				key = 'import'
			else:
				key='export'

			information_DB[key] = split_line#asignamos a la key un arreglo que contiene la informacion de la linea (host, db, user, pass)
			line = file_read.readline()#leemos siguiente linea y aumentamos contador
			cont+=1
		file_read.close()
	except:#manejo de exceptions, en caso de fallar se termina el script
		syslog.syslog(syslog.LOG_INFO,"No es posible abrir el archivo con la informacion de la conexion a la base de datos")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

	return information_DB#se retorna el diccionario con la informacion recabada

#funcion que permite la conexion con la base de datos, los parametros representan:
#host => el host a conectarse, descrito por la ip
#dbname => nombre de la base de datos
#user => usuario con el cual se accedera a la base de datos
#password => clave para el usuario que se conectara a la base de datos
def ConnectDB (host, dbname, user, password):

	connection = "host= %s dbname= %s user=%s password=%s" % (host, dbname, user, password)#conexion con la informacion
	var = "Connecting to database->%s" % (connection)#se muestra en el log la informacion 
	syslog.syslog(syslog.LOG_INFO,var)

	try: #manejo de try/exception
		conn = psycopg2.connect(connection)#se obtiene la conexion
		cursor_db = conn.cursor()#se recupera un cursor
		syslog.syslog(syslog.LOG_INFO,"Connected!")
	except:#manejo de exception
		syslog.syslog(syslog.LOG_INFO,"No es posible conectarse a la base de datos")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	
	return conn, cursor_db#se retorna la conexion y el cursor de la conexion

#funcion que permite determinar si un elemento en particular existen en la base de datos a exportar la informacion
#data => representa la data a buscar en la base de datos
#cursor_db => representa la conexion con la base de datos 
#table => representa la tabla bajo la cual se hara la consulta...
#field => el campor a seleccionar para la consulta...
def CheckExistenceInDB(cursor_db, table, field, data):

	try:#manejo try/exceot
		new_data = data.upper()#transformamos todo a mayusculas
		query = "SELECT * FROM %s where %s = '%s'" % (table, field, new_data)#hacemos la consulta en base a lo solicitado
		cursor_db.execute(query)#ejecutamos
		cont=0#determina si existe elemento o no

		for row in cursor_db:#recorremos resultado y aumentamos el contador a medida que avance el ciclo
			cont+=1
			break
	except:#manejo de exceptions, ante cualquier incoveniente se cierra el script
		syslog.syslog(syslog.LOG_INFO,"No es posible hacer una consulta a la base de datos (CheckExistenceInDB)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	
	return cont#retornamos el contador

#funcion que inserta elementos en la base de datos a la cual se exportaran los elementos...
#connection => representa la conexion con la base de datos
#cursor_db => representa el cursor de la conexion a la base de datos
#table => tabla que se insertara un elemento
#data => informacion que sera almacenada en la tabla
#sequence => secuencia que representa el valor de la clave primaria y es una funcion en el sql cargado en la base de datos
#para generar ids autoincrementables
def InsertDataInDB(connection, cursor_db, table, data, sequence):

	try:#manejo de exceptions
		query = ""#consulta vacia
		query_insert_estadousuario = ""#en case que la tabla sea estadodispositivo el contenido tambien se copiara en la tabla estadousuario
		if table == "estadodispositivo":#para determinar que tipo de tabla es
			description = data.split(" ")#determinar si es activo o no
			desc = ""
			if (description[0] == "desactivado" or description[0] == "Desactivado"):#evaluamos el estado y cambiamos a mayusculas
				desc = "DESHABILIDATO"
			else:
				desc = "HABILITADO"
			data_new = data.upper()#convertimos a mayuscula la informacion
			query = "INSERT INTO %s VALUES (nextval('%s'),'%s', '%s')" % (table, sequence, data_new, desc)#hacemos la query general y para el estadousuario		
			query_insert_estadousuario = "INSERT INTO estadousuario values (nextval('estadousuario_id_seq'),'%s', '%s')" % (data_new, desc)
		else:#en caso de que sea otra tabla...
			new_data = data.upper()
			query = "INSERT INTO %s VALUES (nextval('%s'),'%s')" % (table, sequence, new_data)#hacemos el insert en caso de contrario
		cursor_db.execute(query)#ejecutamos la accion
		#ejecucion en la tabla estadousuario
		if query_insert_estadousuario != "":
			cursor_db.execute(query_insert_estadousuario)		
		connection.commit()#actualizamos
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la insercion en la base de datos (InsertDataInDB)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#funcion que obtiene los elementos de la base de datos import y los agrega a una lista de elementos
#cursor_db => cursor de la conexion de la base de datos
#table => tabla a la cual se esta realizacion la consulta
#index => valor de la posicion a capturar la respuesta a la consulta (un elemento en particular)
def GetElementesToInsert(cursor_db, table, index):

	try:
		query = "SELECT * FROM %s " % table#consulta a realizarse 
		list_response = []#para almacenar la respuesta
		cursor_db.execute(query)

		#se obtienen los elementos de la consulta
		for row in cursor_db:
			list_response.append(row[index])
	
	except:#manejo de errores
		syslog.syslog(syslog.LOG_INFO,"No es posible hacer la consulta a la base de datos (GetElementesToInsert)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

	return list_response

#function que completa el proceso: hace consulta para obtener elementos a insertar en DB import, 
#chequea su existencia en la base de datos export
#cursor_db_export => cursor de la conexion con la base de datos a migrar la informacion
#connection_db_export => conexion con la base de datos a migrar la informacion
#cursor_db_import => cursro de la conexion con la base de datos a exportar la informacion
#connection_db_import => conexion con la base de datos a exportar la informacion
#table_import => tabla desde la cual se extraera la informacion
#table_export => tabla a la cual se ingresaran datos
#field_export => campo que se ingresara datos
#sequence => valor del ide de la tabla que corresponde a la clave primaria y hace referencia a una funcion sql
#index => posicion del campo que contiene la informacion segun consulta realizada
def CompleteProcessInsertion(cursor_db_export, connection_db_export, cursor_db_import, connection_db_import, table_import, table_export, field_export, sequence, index):

	list_response = GetElementesToInsert(cursor_db_import, table_import, index)#obtenemos la lista de respuesta

	print "Completando informacion de %s" % table_export
	widgets = ['Progreso: ', Percentage(), ' ', Bar(marker=RotatingMarker()), ' ', ETA(), ' ', FileTransferSpeed()]#widgets para la barra de progreso
	pbar = ProgressBar(widgets=widgets, maxval=len(list_response)).start()#comienzo de la barra de progreso

	i=0#para ir actualizando la barra
	for element in list_response:#ciclo que recorre la respuesta obtenido por la consulta

		var = ""
		if (CheckExistenceInDB(cursor_db_export, table_export, field_export, element) == 0):#determinamos si existe el elemento
			var= "insert element %s en tabla %s " % (element, table_export)
			InsertDataInDB(connection_db_export, cursor_db_export, table_export, element, sequence)#se inserta en caso de que no exista
		else:
			var= "element %s already exists in tabla %s" % (element, table_export)#se reporta en caso contrario
		syslog.syslog(syslog.LOG_INFO,var)
		pbar.update(i+1)#se actualzia la barra de progreso
		i+=1
	pbar.finish()#finaliza la barra de progreso

def main ():#funcion principal

	logs = open("FilesOutput/CompleteTables_Logs_error.txt", 'w')#se abre archivo log

	#abrimos syslog para registrar lo que realiza el script...
	syslog.openlog("CompleteTables.py", syslog.LOG_USER)

	syslog.syslog(syslog.LOG_INFO,"this script has created the next output files CompleteTables_Logs_error.txt")

	information_DB = ReadCSV(sys.argv[1])#obtenemos la informacion de la base de datos...

	syslog.syslog(syslog.LOG_INFO, "Completando datos en tablas no relacionadas")
	#se generan las conexiones
	data_connected_import = ConnectDB(information_DB['import'][0], information_DB['import'][1], information_DB['import'][2], information_DB['import'][3])
	data_connected_export = ConnectDB(information_DB['export'][0], information_DB['export'][1], information_DB['export'][2], information_DB['export'][3])

	#se realiza el mismo proceso para las diferentes tablas...
	#insertando elementos tabla tipodispositivo
	CompleteProcessInsertion(data_connected_export[1], data_connected_export[0], data_connected_import[1], data_connected_import[0], "tipo_dispositivo", "tipodispositivo", "tipo", "tipodispositivo_id_seq", 1)
	#insertando elementos tabla marca
	CompleteProcessInsertion(data_connected_export[1], data_connected_export[0], data_connected_import[1], data_connected_import[0], "marca", "marca", "marca", "marca_id_seq", 1)		
	#insertando elementos tabla tipousuario
	CompleteProcessInsertion(data_connected_export[1], data_connected_export[0], data_connected_import[1], data_connected_import[0], "tipo_usuario", "tipousuario", "tipo", "tipousuario_id_seq", 0)	
	#insertando elementos tabla estadodispositivo and tabla estadousuario
	CompleteProcessInsertion(data_connected_export[1], data_connected_export[0], data_connected_import[1], data_connected_import[0], "estado", "estadodispositivo", "descripcion", "estadodispositivo_id_seq", 1)	
	
	logs.write("ok")
	logs.close()

	syslog.closelog()#cerramos syslog
	return 0	

#llamada a la funcion principal
if __name__ == '__main__':
	main()

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

	usuarios_vencidos = open("FilesOutput/usuarios_vencidos.csv", 'w')
	usuarios_vencidos.close()
	estadisticas_finales = open("FilesOutput/Estadisticas_Finales.txt", 'a')
	estadisticas_finales.close()
	usuarios_dos_equipos = open("FilesOutput/UsuariosDosEquipos.csv", 'w')
	usuarios_dos_equipos.close()

#funcion que nos permite poder obtener las estadisticas con respecto a los usuarios, en este caso la cantidad de usuarios insertadis
#totales y por tipo, asi como tambien los usuarios que se encuentran vencidos y los envia a un archivo csv...
#cursor_db => representa el cursor de acceso a la informacion a la base de datos
#connection_db => representa la conexion a la base de datos establecida
def EstadisticasUsuarios(cursor_db, connection_db):

	#abrimos el archivo de estadisticas finales...
	estadisticas_finales = open("FilesOutput/Estadisticas_Finales.txt", 'a')
	usuarios_vencidos_files = open("FilesOutput/usuarios_vencidos.csv", 'a')
	estadisticas_finales.write("Estadisticas Usuarios\n")

	syslog.syslog(syslog.LOG_INFO,"Realizando estadisticas de los usuarios insertados")
	#total usuarios insertados 
	query = "select COUNT(*) as cantidad from usuario"
	total_usuarios = 0
	cursor_db.execute(query)#hacemos la consulta y obtenemos la cantidad de usuarios
	for row in cursor_db:
		total_usuarios = row[0]

	var = "Total usuarios insertados: " + str(total_usuarios)
	estadisticas_finales.write(var+"\n")
	syslog.syslog(syslog.LOG_INFO, var)
	#total usuarios por tipo 
	query = "select idtipousuario,  tipo, COUNT(*) as cantidad from usuario join tipousuario on (usuario.idtipousuario = tipousuario.id) group by idtipousuario, tipo"
	cursor_db.execute(query)

	estadisticas_finales.write("\nTotal Usuarios Insertados por Tipo\n")
	syslog.syslog(syslog.LOG_INFO, "Total Usuarios Insertados por Tipo")
	estadisticas_finales.write("Tipo Usuario\tCantidad\n")
	syslog.syslog(syslog.LOG_INFO,"Tipo Usuario Cantidad")

	#obtenemos los resultados por tipo y los escribimos en el archivo estadisticas...
	for row in cursor_db:
		elementos = []#almacenamos los resultados...
		for element in row:
			elementos.append(element)
		var = str(elementos[1])+"\t"+str(elementos[2])+"\n"#formamos la linea y la escribimos
		estadisticas_finales.write(var)
		syslog.syslog(syslog.LOG_INFO,var)
	#total usuarios vencidos por tipo, ademas estos se mandan a un csv con dicha informacion...
	query = "select * from usuario where fechatermino < '2015-08-31'"
	cursor_db.execute(query)

	syslog.syslog(syslog.LOG_INFO, "Identificando usuarios vencidos")
	for row in cursor_db:#obtenemos los resultados de la consulta y generamos el csv con la informacion de dichos usuarios....
		elementos = []
		for element in row:
			elementos.append(element)

		#armamos la linea y la escribimos en el csv...
		var = str(elementos[3])+";"+str(elementos[4])+";"+str(elementos[5])+";"+str(elementos[1])+";"+str(elementos[2])+"\n"
		usuarios_vencidos_files.write(var)

	#total de los usuarios vencidos...
	query = "select COUNT (*) from usuario where fechatermino < '2015-08-31'"
	cursor_db.execute(query)
	total_usuarios_vencidos =0
	for row in cursor_db:
		total_usuarios_vencidos = row[0]

	var = "\nTotal Usuarios Vencidos (2015-08-31): "+ str(total_usuarios_vencidos)+"\n"
	estadisticas_finales.write(var)
	syslog.syslog(syslog.LOG_INFO, var)
	#total usuarios con mas de dos equipos => mandarlos a CSV
	#query = "drop view if exists cantidad_equipos_usuarios"#borramos la vista si es que existe...
	#cursor_db.execute(query)
	#connection_db.commit()

	#creamos una vista para contar los equipos por usuarios...
	query = "create view cantidad_equipos_usuarios as select COUNT(*) as cantidad, idusuario as id_user from usuariosequipos group by idusuario"
	cursor_db.execute(query)
	connection_db.commit()

	total_usuarios_equipos = 0
	query = "select COUNT(*) from usuario where id in (select id_user from cantidad_equipos_usuarios where cantidad >2)"#hacemos la consulta para obtener los usuarios con mas de dos equipos asociados...
	cursor_db.execute(query)
	for row in cursor_db:
		total_usuarios_equipos = row[0]

	var = "Total usuarios con mas de dos equipos asociados: " + str(total_usuarios_equipos)+"\n"
	estadisticas_finales.write(var)#escribimos la informacion en el archivo de texto...
	syslog.syslog(syslog.LOG_INFO, var)
	#obtenemos la informacion de los usuarios que tienen mas de dos equipos asociados y la enviamos a un csv....
	query = "select * from usuario where id in (select id_user from cantidad_equipos_usuarios where cantidad >2)"
	usuarios_dos_equipos = open("FilesOutput/UsuariosDosEquipos.csv", 'a')

	cursor_db.execute(query)
	#recorremos el resultado de la consulta y formamos las lineas para escribir el csv...
	for row in cursor_db:
		elementos = []
		for element in row:
			elementos.append(element)

		#armamos la linea y la escribimos en el csv...
		var = str(elementos[3])+";"+str(elementos[4])+";"+str(elementos[5])+";"+str(elementos[1])+";"+str(elementos[2])+"\n"
		usuarios_dos_equipos.write(var)#escribimos en el csv...

	#cerramos todos los archivos ocupados...
	usuarios_dos_equipos.close()
	usuarios_vencidos_files.close()
	estadisticas_finales.close()

#funcion que permite escribir las estadisticas asociadas a los equipos y a las macs...
#cursor_db => representa el cursor de acceso a la informacion a la base de datos
#connection_db => representa la conexion a la base de datos establecida
def EstadisticasEquiposMacs(cursor_db, connection_db):

	estadisticas_finales = open("FilesOutput/Estadisticas_Finales.txt", 'a')
	syslog.syslog(syslog.LOG_INFO,"Estadisticas Equipos")
	estadisticas_finales.write("\nEstadisticas Equipos\n")

	syslog.syslog(syslog.LOG_INFO,"Realizando las estadisticas de los equipos y macs ingresadas")
	#total equipos insertados => 
	query = "select COUNT(*) as cantidad from equipo"
	total_equipos = 0
	cursor_db.execute(query)
	for row in cursor_db:
		total_equipos = row[0]
	var = "Total Equipos Insertados: "+str(total_equipos)+"\n"
	syslog.syslog(syslog.LOG_INFO, var)
	estadisticas_finales.write(var)
	syslog.syslog(syslog.LOG_INFO, "Total Equipos Insertados por Tipo de Usuario:")
	estadisticas_finales.write("\nTotal Equipos Insertados por Tipo de Usuario:\n")
	syslog.syslog(syslog.LOG_INFO, "Tipo Usuario Cantidad Equipos")
	estadisticas_finales.write("Tipo Usuario\tCantidad Equipos\n")

	#total equipos insertados por tipo usuario
	query = "select usuario.idtipousuario, tipousuario.tipo, COUNT(*) as cantidad from equipo join usuariosequipos on (usuariosequipos.idequipo = equipo.id) join usuario on (usuario.id = usuariosequipos.idusuario) join tipousuario on (usuario.idtipousuario = tipousuario.id) group by usuario.idtipousuario, tipousuario.tipo"
	cursor_db.execute(query)
	for row in cursor_db:#recorremos la consulta y almacenamos los valores, los dejamos en una variable para armar la linea del archivo...
		elementos = []
		for element in row:
			elementos.append(element)
		#armamos la linea y escribimos en el archivo..
		var = str(elementos[1])+"\t"+str(elementos[2])+"\n"#formamos la linea y la escribimos
		syslog.syslog(syslog.LOG_INFO, var)
		estadisticas_finales.write(var)

	#total macs insertadas => 
	query = "select COUNT(*) as cantidad from macs"
	total_macs = 0
	cursor_db.execute(query)
	for row in cursor_db:
		total_macs = row[0]
	var = "Total de Macs Insertadas: "+str(total_macs)+"\n"
	estadisticas_finales.write(var)#obtenemos la informacion y la escribimos en el archivo de estadisticas finales...
	syslog.syslog(syslog.LOG_INFO, var)
	#total macs insertadas por tipo =>  
	query = "select tipointerfaz, COUNT(*) as cantidad from macs group by (tipointerfaz)"
	cursor_db.execute(query)

	estadisticas_finales.write("\nTotal de Macs insertadas por tipo de interfaz:\n")
	syslog.syslog(syslog.LOG_INFO, "Total de Macs insertadas por tipo de interfaz")
	estadisticas_finales.write("Tipo de Interfaz\tTotal Macs\n")
	syslog.syslog(syslog.LOG_INFO,"Tipo de Interfaz Total Macs")
	#recorremos el resultado de la consulta
	for row in cursor_db:
		elementos = []
		for element in row:
			elementos.append(element)
		var = str(elementos[0])+"\t"+str(elementos[1])+"\n"
		estadisticas_finales.write(var)
		syslog.syslog(syslog.LOG_INFO, var)
	#cerramos los archivos que se estan ocupando...
	estadisticas_finales.close()

#generacion de la funcion principal
def main ():
	
	#abrimos syslog para registrar lo que realiza el script...
	syslog.openlog("EstadisticasFinales.py", syslog.LOG_USER)

	CreateFileOuputs()#creamos los archivos de salida
	information_DB = ReadCSV(sys.argv[1])#obtenemos la informacion de la base de datos...
	data_connected_import = ConnectDB(information_DB['import'][0], information_DB['import'][1], information_DB['import'][2], information_DB['import'][3])#conexion db import
	data_connected_export = ConnectDB(information_DB['export'][0], information_DB['export'][1], information_DB['export'][2], information_DB['export'][3])#conexion db export

	#llamamos a las funciones que generan las estadisticas...
	EstadisticasUsuarios(data_connected_export[1], data_connected_export[0])
	EstadisticasEquiposMacs(data_connected_export[1], data_connected_export[0])
	estadisticas = open ("FilesOutput/Estadisticas_Error.txt", 'w')
	estadisticas.write("ok")
	estadisticas.close()

	syslog.closelog()#cerramos syslog

if __name__ == '__main__':
		main()	
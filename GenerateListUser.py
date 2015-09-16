#!/usr/bin/python

# GenerateListUser.py, script que permite la realizacion de un archivo csv con la 
# informacion de aquellos equipos asociados a alguna mac que exista en la base de datos
# actual pero que no forme parte del csv recibido.
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

#funcion que permite crear los archivos de salida que se requieren para este script
def CreateFileOuputs():

	list_usuarios_not_File = open("FilesOutput/List_Equipos_usuarios_not_File.txt", 'w')#archivo con la lista de usuarios no existences en csv
	list_usuarios_not_File.close()

#funcion que permite hacer la lectura y obtener todas las posibles macs a ser insertadas en la base de datos export, las cuales provienen del csv
#name_file => representa el nombre del archivo
def ReadCSV (name_file):
	
	list_macs = []#estructura que almacenara las macs del csv
	
	try:	
		file_read = open(name_file)#abrimos archivo
		line = file_read.readline()#leemos primera linea
		while line:#ciclo de lectura
			split_line = line.split(',')#separamos por ,
			list_macs.append(split_line[0])#obtenemos la primera posicion
			line = file_read.readline()#avanzamos
		file_read.close()#cerramos ciclo...
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible abrir el archivo que contiene las macs")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return list_macs#se retorna la estructura con las macs

#funcion que permite poder obtener las macs existentes en la base de datos y las retorna en una estructura...
#cursor_db => representa el cursor de la conexion con la base de datos
def GetMacsInDB(cursor_db):

	list_MacsInDB = []#estructura para las macs

	try:
		query = "SELECT * FROM macs"#consulta
		cursor_db.execute(query)#ejecucion
		for row in cursor_db:#ciclo de respuesta
			list_MacsInDB.append(row[0])
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetMacsInDB)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return list_MacsInDB#se retorna la lista obtenida		

#funcion que permite comparar aquellas macs que existen en la base de datos pero no se encuentran en el archivo csv, 
#retorna una estructura con dichas macs...
#list_macs => lista de macs en csv
#list_MacsInDB => lista de macs en la base de datos
def GetMacsNotInFile(list_macs, list_MacsInDB):

	list_macs_notInFile =[]#estructura que contendra las macs
	maximo = len(list_MacsInDB) * len(list_macs)#largo maximo para la barra de progreso
	pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=maximo).start()#barra de progreso
	i=0#para ir actualizando la barra de progreso
	for mac in list_MacsInDB:#consulto por cada mac que esta en la base de datos y verifico si existe en el csv...
		cont=0#verifica existencia 
		for element_macs in list_macs:#busco en la lista de los csv...
			i+=1#aumento el actualziador
			if mac == element_macs:#hago la comparacion...
				cont+=1#lo encontro...
				break
			pbar.update(i+1)#actualizamos
		if cont == 0:#preguntamos si encontro la mac de la base de datos en el archivo csv, si se cumple agregamos a lista
			list_macs_notInFile.append(mac)
	pbar.finish()#finalizamos proceso
	return list_macs_notInFile#retornamos la lista	

#funcion que permite poder hacer la consulta para obtener los datos del usuario y generar un diccionario con dicha informacion 
#para poder generar el csv con los datos: rut, nom1, nom2, apep, apem, fecharegistro
def GetInfoForGenerateCSVOutput(list_macs_notInFile, cursor_db):

	info_dicctionary = {}#diccionario con la informacion
	try:
		for mac in list_macs_notInFile:#recorremos las macs 
			query = "select usuario.rut, usuario.nom1, usuario.nom2, usuario.apep, usuario.apem, usuario.fecha_ini, usuario.fecha_fin, usuario.id_tipo_us, dispositivo_usuario.serial from usuario join dispositivo_usuario on (usuario.rut = dispositivo_usuario.rut) join macs on (macs.serial = dispositivo_usuario.serial) where macs.mac = '%s'" % mac#hacemos la consulta
			cursor_db.execute(query)#ejecucion
			for row in cursor_db:#para cada respuesta guardamos como clave la mac y su informacion en el diccionario
				info_dicctionary[mac] = row
				break
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (GetInfoForGenerateCSVOutput)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return info_dicctionary#se retorna el diccionario

#funcion que genera el csv en base a la informacion capturada por el dicctionario...
#info_ficctionary => representa la informacion de cada usuario asociada a una mac en particular
def GenerateCSV(info_dicctionary):

	print "Creando CSV con usarios existentes en base de datos y no en file csv de macs..."
	syslog.syslog(syslog.LOG_INFO,"Creando CSV con usarios existentes en base de datos y no en file csv de macs...")
	csv = open("FilesOutput/List_Equipos_usuarios_not_File.txt", 'a')#abrimos archivo de escritura
	for key in info_dicctionary:#para cada mac
		line = ""#creamos una linea
		cont=0#determina que parte de linea se esta formando
		for element in info_dicctionary[key]:#para cada elemento en el arreglo asociado a la mac
			elemento = str(element)#convierto a string
			if cont != 0:#determino que parte de la linea es
				line = line + ";"+elemento
			else:
				line = line + elemento
			cont+=1#aumento el reconocedor
		csv.write(line+"\n")#escribo
	csv.close()#cierro archivo

#funcion principal
def main ():

	#abrimos syslog para registrar lo que realiza el script...
	syslog.openlog("GenerateListUser.py", syslog.LOG_USER)

	CreateFileOuputs()#generacion de archivos de salida	
	data_connected = ConnectDB(sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])#conexion a la base de datos
	list_macs = ReadCSV(sys.argv[1])#lectura del csv
	list_MacsInDB = GetMacsInDB(data_connected[1])#obtencion de macs en base de datos
	list_MacsDBNotFile = GetMacsNotInFile(list_macs, list_MacsInDB)#macs en base de datos no en csv
	info_dicctionary = GetInfoForGenerateCSVOutput(list_MacsDBNotFile, data_connected[1])#diccionario con la informacion de las macs anteriores
	GenerateCSV(info_dicctionary)#generacion de csv con la informacion del diccionario
	syslog.closelog()#cerramos syslog
	return 0

#llamada a funcion principal	
if __name__ == '__main__':
	main()
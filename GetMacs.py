#!/usr/bin/python

# GetMacs.py, script que permite hacer las capturas de las macs existentes en la base 
# de datos a exportar y que existen en el csv de entrada, genera un archivo de texto
# con aquellas macs que no existen en la base de datos a exportar la informacion y las cuales
# son las que se deben ingresar a la nueva base de datos.
# Genera un archivo log con los enventos que ocurren al momento de realizar la ejecucion del script
# como resumen entrega la informacion de cuantas macs existen en la base de datos y que corresponden
# a las que estan en el csv, ademas de las que no existen en la base de datos y si en el csv, finalmente 
# presenta una barra de progreso para determinar cuanto se demora en generar el proceso de ejecucion 
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

#modulos a utilizar
import sys
import psycopg2
import time
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

	connection = "host= %s dbname= %s user=%s password=%s" % (host, dbname, user, password)#conexion con la informacion dada
	var = "Connecting to database %s" % (connection)#impresion de los datos de la conexion
	syslog.syslog(syslog.LOG_INFO,var)

	#se solicitara la conexion por medio de un tratamiento try/except, en caso de ocurrir algun tipo de problema
	try: 
		conn = psycopg2.connect(connection)	#se establece la conexion por medio del modulo asociado
		cursor_db = conn.cursor()#se obtiene un cursor con los datos obtenidos
		syslog.syslog(syslog.LOG_INFO,"Connected!")
	except:
		#se escribe en el log el error y en se finaliza el script
		syslog.syslog(syslog.LOG_INFO,"error, no es posible conectarse a la base de datos")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	
	return conn, cursor_db#se retorna la conexion y el cursor

#funcion que permite la creacion de los outputs que se generan en la ejecucion del script
def CreateFileOuputs():

	list_macs_not_exists = open("FilesOutput/GetMacslist_macs_not_exists.txt", 'w')#archivo para la lista de macs que no existen en la base de datos a exportar
	list_macs_not_exists.close()
	list_macs_exists = open("FilesOutput/GetMacslist_macs_exists.txt", 'w')#archivo para la lista de macs que existen en la base de datos a exportar
	list_macs_exists.close()

#funcion que lee un csv y obtiene la informacion de las macs
#name_file => representa el nombre de archivo que se extraera la informacion
def ReadCSV (name_file):
	
	list_macs = {}#guarda la informacion de las macs
	
	#manejo de errores y excepciones
	try:	
		
		file_read = open(name_file)#abrimos archivo
		line = file_read.readline()#leemos la primera linea

		while line:#ciclo de lectura
		
			split_line = line.split(',')#el csv posee como formato una ",", por lo que hacemos el split por dicho simbolo
			mac = split_line[0]#obtenemos la mac y seguimos leyendo la siguiente linea
			list_macs[mac] = split_line[1]#al diccionario le agregamos como clave la mac y como elemento el nombre del equipo
			line = file_read.readline()

		file_read.close()#cerramos archivo
	except:#capturamos el error y en caso de ocurrir un error finalizamos el script
		syslog.syslog(syslog.LOG_INFO,"no es posible abrir el archivo con la informacion de las macs")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

	return list_macs#se retorna la lista de macs obtenidas...

#funcion que permite determinar si una mac en particular existen en la base de datos a exportar la informacion
#mac => representa la mac a buscar en la base de datos
#cursor_db => representa la conexion con la base de datos 
#table => representa la tabla bajo la cual se hara la consulta...
#field => el campor a seleccionar para la consulta...
def CheckMacInDB(mac, cursor_db, table, field):

	query = "SELECT * from %s where %s = '%s'" % (table, field, mac)#hacemos la consulta

	try:#con un contador determinamos si existe el elemento seleccionado o no...
		cursor_db.execute(query)#ejecutamos la consulta
		cont=0

		for row in cursor_db:#contamos las veces que ingresa al for 
			cont+=1
			break
	except:
		syslog.syslog(syslog.LOG_INFO,"No es posible realizar la consulta a la base de datos (CheckMacInDB)")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")	
	
	return cont#retornamos el valor del contador

#funcion que permite la generacion de un archivo con los datos que existen asociados a las macs
def GenerateFileOutput(list_macs, name_file):

	file_output = open(name_file, 'a')#abrimos archivo de escritura

	for mac in list_macs:#recorremos todas las macs...
	
		file_output.write(mac)#escribimos en archivo
	file_output.close()

#funcion principal
def main ():

	#abrimos syslog para registrar lo que realiza el script...
	syslog.openlog("GetMacs.py", syslog.LOG_USER)
	#generamos los archivos de salida
	CreateFileOuputs()	
	logs = open("FilesOutput/GetMacslogs_error.txt", 'w')
	data_conected = ConnectDB(sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])
	syslog.syslog(syslog.LOG_INFO,"este script ha creado los siguientes archivos de salida FilesOutput/GetMacslist_macs_not_exists.txt FilesOutput/GetMacslist_macs_exists.txt")
	
	#leemos el csv y obtenemos el diccionario...
	list_macs = ReadCSV(sys.argv[1])
	list_macs_not_exists = []#contendra las macs que no existen en la base de datos a exportar la informacion
	list_macs_exists = []#contendra las macs que existen en la base de datos a exportar

	#chequea si la mac existe en la base de datos
	print "chequeando macs"
	widgets = ['Progreso: ', Percentage(), ' ', Bar(marker=RotatingMarker()), ' ', ETA(), ' ', FileTransferSpeed()]#widget para la barra de progreso
	pbar = ProgressBar(widgets=widgets, maxval=len(list_macs)).start()#iniciamos la barra de progreso

	i=0#para el aumento de la barra de progreso
	for mac in list_macs:
		var = mac+";"+list_macs[mac]
		if (CheckMacInDB(mac, data_conected[1],"macs", "mac")== 0):
			list_macs_not_exists.append(var)
		else:
			list_macs_exists.append(var)
		pbar.update(i+1)
		i+=1
	pbar.finish()

	#generate file whit summary
	GenerateFileOutput(list_macs_not_exists, "FilesOutput/GetMacslist_macs_not_exists.txt")
	GenerateFileOutput(list_macs_exists, "FilesOutput/GetMacslist_macs_exists.txt")
	logs.write("ok")
	logs.close()

	syslog.closelog()#cerramos syslog

	return 0

if __name__ == '__main__':
	main()
#!/usr/bin/python

# VerificaRequisitos.py, script que permite verificar los siguientes requisitos:
# 
# 1. Verifica ser usuario root
# 2. Verifica conexion a la base de datos export
# 3. Verifica conexion a la base de datos import
# 
# La recepcion de los parametros es con respecto al argparse, los parametros que se reciben son dos string
# conformados tipo csv separado la informacion con ;. Estos representan la informacion
# de los datos para conectarse con la base de datos tanto import como export, esta informacion proviene de
# un archivo en formato csv, el cual es el archivo que se recibe desde terminal.
# 
# Crea un archivo de texto con el cual se verifica si todos los pasos son correctos, 
# en tal caso se proceede a continuar con la ejecucion, en caso contrario se termina 
# la ejecucion y no se procede con la ejecucion de los otros scripts.
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
import psycopg2
import sys
import os
import syslog

#funcion que permite verificar si es posible conectarse a la base de datos que esta recibiendo como parametro
#host => el host a conectarse, descrito por la ip
#dbname => nombre de la base de datos
#user => usuario con el cual se accedera a la base de datos
#password => clave para el usuario que se conectara a la base de datos
def Verificacion(host, data_base, user, password):

	connection = "host= %s dbname= %s user=%s password=%s" % (host, data_base, user, password)#conexion con la informacion dada
	var = "Connecting to database ->%s" % (connection)#impresion de los datos de la conexion
	syslog.syslog(syslog.LOG_INFO,var)
	#se solicitara la conexion por medio de un tratamiento try/except, en caso de ocurrir algun tipo de problema
	try: 
		conn = psycopg2.connect(connection)	#se establece la conexion por medio del modulo asociado
		cursor_db = conn.cursor()#se obtiene un cursor con los datos obtenidos
	except:
		#se escribe en el log el error y en se finaliza el script
		syslog.syslog(syslog.LOG_INFO,"error, no es posible conectarse a la base de datos")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")

#funcion que permite verificar si el usuario que esta accediendo es del tipo root...
def VerificaUsuario():

	#se verifica si el usuario es root...
	if not os.geteuid() == 0:
		syslog.syslog(syslog.LOG_INFO,"error, debes ser usuario root para ejecutar el script")
		return "no"
	else:
		syslog.syslog(syslog.LOG_INFO,"correcto, eres usuario root")
		return "ok"

#llamada a la funcion principal del sistema
def main ():

	#abrimos syslog para registrar lo que realiza el script...
	syslog.openlog("VerificaRequisitos.py", syslog.LOG_USER)
	#abrimos archivo de texto con la informacion de la conexion...
	file_verificacion = open("FilesOutput/Verificacion.txt", 'w')#con w para crearlo

	#recibimos los argumentos
	data_import = sys.argv[2].split('_')
	data_export = sys.argv[1].split('_')

	syslog.syslog(syslog.LOG_INFO,"Verificando las conexiones a la base de datos")
	#hacemos la llamada a la funcion...
	Verificacion(data_export[0],data_export[1], data_export[2], data_export[3])#verificamos la conexion a la base de datos export
	Verificacion(data_import[0], data_import[1], data_import[2], data_import[3])#verificamos la conexion a la base de datos import

	#hacemos la verificacion de usuario root...
	usuario_root = VerificaUsuario()

	if (usuario_root == "ok"):
		file_verificacion.write("ok")#si todos los procesos terminan de ejecutarse de manera correcta simplemente se escribe un ok para el archivo
	else:
                file_verificacion.write("no")#hubo algun problema
			
	syslog.closelog()#cerramos syslog

#llamada a la funcion principal
if __name__ == '__main__':
	main()

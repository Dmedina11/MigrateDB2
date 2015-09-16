#!/usr/bin/python

# HandlerMigrateData.py, script que permite mantener el control de la ejecucion de los
# scripts por separado, es quien determina si el script se ejecuta o no y evalua el
# siguiente paso a seguir, recibe como argumento el csv con la informacion de las macs
# y el archivo con la informacion de las conexiones, entrega un log con lo que ocurre
# en cada etapa, ademas de los logs que se generan por separado, crea un directorio en
# el cual se depositan los outputs.
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
import argparse
import syslog
import time	
#funcion que por medio de la lectura de los archivos de salida de los scripts determina si se ejecuta el siguiente paso
#correspondiente en el pipe line establecido
#name_file => representa el nombre del archivo a extraer la informacion
def CheckExecuteNextStep(name_file):

	option=0#para determinar si se continua o no...
	
	try:		
		file_read = open(name_file)#abrimos archivo
		line = file_read.readline()#leemos primera linea
		while line:

			if line == "ok":#buscamos la linea que diga ok
				option=1
				break
			line = file_read.readline()

		file_read.close()#cerramos el archivo de texto

	except:#manejo de errores
		syslog.syslog(syslog.LOG_INFO, "No es posible determinar la continuacion del script debido a problemas con el archivo log del script anterior")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	return option

#funcion que permite revisar si la informacion para las conexiones a la base de datos proveniente del csv recibido es correcta
#o no, es decir, si vienen todos los elementos, si esta bien formado, y cosas por el estilo.
#name_file => es el nombre del archivo que se recibe por la linea de comando 
def CheckFileDataDB(name_file):

	long_data = 0#largo de los datos
	cont=0#cantidad de lineas

	try:	
		file_read = open(name_file)#abrimos..
		line = file_read.readline()#leemos primera linea
		last_line =""#para almacenar la ultima linea => conexion a base de datos import
		first_line = line#para almacenar la primera => conexion a base de datos export

		while line:#ciclo de lectura

			new_line = line.replace("\n", "")#quitamos el enter
			cont+=1#aumentamos la cantidad de lineas
			elements = new_line.split(';')#hacemos el split para determinar la cantidad de elementos, estos deben ser 4
			if len(elements)!=4:

				syslog.syslog(syslog.LOG_INFO, "cantidad de elementos es distinta a lo requerido (4)")
				sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
			if cont>3:#chequeamos la cantidad de lineas
				syslog.syslog(syslog.LOG_INFO, "cantidad de lineas en archivo es distinta a la requerida (2)")
				sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
			
			last_line = line#actualizamos la ultima linea y seguimos leyendo				
			line = file_read.readline()

		file_read.close()
	except:#manejo de errores
		syslog.syslog(syslog.LOG_INFO,"no es posible evaluar la informacion del csv")
		sys.exit("script interrumpido, favor revisar archivo LOG: var/log/syslog")
	
	last_line = last_line.replace('\n', '')#a la ultima linea le quitamos el enter y formamos un arreglo con sus elementos, lo mismo para la primera
	elements_connection = last_line.split(';')
	first_line = first_line.replace('\n', '')
	elements_connection_import = first_line.split(';')
	
	#se retornan informacion sobre las conexiones de bases de datos de import y export, asi como tambien la informacion en formato csv
	return elements_connection, elements_connection_import, first_line, last_line

def main ():#funcion principal

	os.system('mkdir FilesOutput')#creamos directorio de salida de archivos...
	#obtenemos la fecha y la hora....
	hora_inicial = time.strftime("%c")
	estadisticas_finales = open("FilesOutput/Estadisticas_Finales.txt", 'w')
	syslog.openlog("HandlerMigrateData.py", syslog.LOG_USER)#abrimos syslog
	var = "inicio de script: "+hora_inicial
	syslog.syslog(syslog.LOG_INFO,var)
	estadisticas_finales.write(var+"\n")

	estadisticas_finales.close()
	list_files = []#para la lista de archivos en el caso de que ingrese esa opcion
	list_files_descartados = []#para la lista de archivos a descartar...

	parser = argparse.ArgumentParser()#agregamos el argparse para el manejo de los argumentos...

	#hacemos la comprobacion de los argumentos...
	#agregamos los agumentos al parser
	parser.add_argument("-p", "--path_csv", type=str, help="direcotorio que contiene los archivos csv", required=True)
	parser.add_argument("-c", "--conexion", type=str, help="ruta absoluta mas nombre de archivo que contiene las configuraciones", required=True)
	parser.add_argument("-l", "--List_csv", type=str, help="lista de archivos con informacion de macs", required=False, nargs='+')
	parser.add_argument("-r", "--resumen", type=int, help="Resumen de informacion a archivo externo\n<1> si. Por defecto en archivo LOG", required=False, default=0)
	ayuda = "Archivos de Resumen a descartar. Por defecto no se descarta ninguno. Archivos Resumenes generados: UsuariosDosEquipos.csv Estadisticas_Finales.txt usuarios_vencidos.csv Macs_Eliminadas.csv List_EquiposConflictivos.txt List_users_Delete.txt List_Equipos_usuarios_not_File.txt"
	parser.add_argument("-g", "--Files_Resumen", type=str, help=ayuda, required=False, nargs='+')
	args = parser.parse_args()

	#obtenemos los nombres de los archivos de la lista entregada en el caso de que existan...
	if (args.List_csv):
		for files in args.List_csv:
			list_files.append(files.split(" "))#separamos por espacion dado a que es una lista completa...

	#obtenemos los nombres de los archivos de la lista entregada en el caso de que existan...
	if (args.Files_Resumen):
		for files in args.Files_Resumen:
			list_files_descartados.append(files.split(" "))#separamos por espacion dado a que es una lista completa...
	
	information_dataexport =CheckFileDataDB(args.conexion)#revisa los datos del csv con la informacion de la conexion y genera un arreglo con informacion
	#de la conexion de export e import de las bases de datos...

	#hacemos la verificacion para ver si es posible conectarse a las bases de datos expuestas.
	base_export = information_dataexport[2]
	base_export = base_export.replace(';', '_')
	base_import = information_dataexport[3]
	base_import = base_import.replace(';', '_')

	var = "python VerificaRequisitos.py %s %s" % (base_export, base_import)#se ejecuta la verificacion de las condiciones
	var2 = "ejecutando %s" % var
	syslog.syslog(syslog.LOG_INFO, var2)
	os.system(var)#ejecutando...

	#preguntamos si es posible ejecutar el siguiente y si es asi lo ejecutamos, este es el GetMacs.py
	if (CheckExecuteNextStep("FilesOutput/Verificacion.txt")==1):

	  	syslog.syslog(syslog.LOG_INFO,"Ejecutando la obtencion de los macs")
		
		# #generamos la lista de archivos a ser considerados para concatenarse en un csv unico y trabajar con el
		lista_csv = ""	
	  	if len(list_files)>0:
	  		for i in list_files:
	  			lista_csv = lista_csv+"\ "+i[0]
	  	if lista_csv == "":#si la lista es vacia es por defecto todos los csv...
	  		lista_csv = "*.csv"
	 	
	  	#ejecucion del primer paso
	  	csv_information = args.path_csv#obtenemos csv

	  	#obtenemos la ruta actual...
	  	os.system("pwd > ruta_actual.txt")

	  	#leemos el archivo creado con la ruta actual...
	  	file_path = open ("ruta_actual.txt" , 'r')
	   	ruta_actual = file_path.readline()#leemos el archivo
	   	ruta_actual = ruta_actual.replace('\n', '')#obtenemos la ruta actual

	   	#ejecutamos el sh con el manejo de las csv (concatenacion en unico archivo global)
	   	csv_information = csv_information.replace(' ', '\ ')#reemplazamos los valores de la ruta si es que existe algun caracter especial
	   	ruta_actual = ruta_actual.replace(' ', '\ ')
	   	var = "sh test_directory.sh %s %s %s" % (csv_information, ruta_actual, lista_csv)
	   	os.system(var)#ejecutamos el sh
	   	os.system("rm ruta_actual.txt")#eliminamos el archivo con la ruta obtenida

	    	#debemos agregar el nombre del archivo generado...
	    	list_path = list(csv_information)

	    	#agregamos el / si es que no lo posee...
	    	if list_path[-1] != '/':
	    		csv_information = csv_information+"/"
	    	#ahora agregamos el nombre que se le da en el script sh...
	   	csv_information = csv_information+"total_macs.csv"
	   	new_execute = args.conexion#y las conexiones
	   	new_execute = new_execute.replace(' ', '\ ')#lo mismo para el caso de la ruta hacia el csv de la conexion a la base de datos
	   	var = "python GetMacs.py %s %s %s %s %s" % (csv_information, information_dataexport[0][0], information_dataexport[0][1], information_dataexport[0][2], information_dataexport[0][3])#ejecutamos el primer paso
	   	var2 =  "execute " + var
	   	syslog.syslog(syslog.LOG_INFO, var2)
	   	os.system(var)
	  	os.system("rm FilesOutput/Verificacion.txt")#eliminamos el archivo auxiliar...
	   	#chequear para la ejecucion del siguiente paso
	   	if (CheckExecuteNextStep("FilesOutput/GetMacslogs_error.txt")==1):

	   		#borramos el archivo creado...
	   		#os.system("rm FilesOutput/GetMacslogs_error.txt")
	   		var = "python CompleteTables.py %s" % new_execute
	   		var2 = "execute " + var
	   		syslog.syslog(syslog.LOG_INFO, var2)
	   		os.system(var)   		
	  		#chequeamos para la ejecucion del siguiente paso, en el cual completamos los usuarios existentes en la base de datos
	   		if (CheckExecuteNextStep("FilesOutput/CompleteTables_Logs_error.txt")==1):

	   			#borramos el archivo creado...
	   			#os.system("rm FilesOutput/CompleteTables_Logs_error.txt")
	   	 		var = "python CompleteUsuarios.py %s %s" % (new_execute, csv_information)
	   	 		var2 = "execute " + var
	   	 		syslog.syslog(syslog.LOG_INFO, var2)
	   	 		os.system(var)
	
	   	 		#chequeamos para la ejecucion del siguiente paso, en el cual completamos los equipos de los usuarios.
	   	 		if (CheckExecuteNextStep("FilesOutput/CompleteUsuarios.txt")==1):
	   				#borramos el archivo creado para verificar la ejecucion del siguiente paso
	   				#os.system("rm FilesOutput/CompleteUsuarios.txt")
	   				var = "python CompleteEquipos.py %s %s" % (new_execute, csv_information)
	   				var2 = "execute " + var
	   				syslog.syslog(syslog.LOG_INFO, var2)
	   				os.system(var)

	   				#chequeamos para la ejecucion del siguiente paso, en el cual completamos las macs de los equipos de los usuarios.
	   				if (CheckExecuteNextStep("FilesOutput/CompleteEquipos.txt")==1):
	   					#borramos el archivo creado para verificar la ejecucion del siguiente paso
	   					#os.system("rm FilesOutput/CompleteEquipos.txt")
	   					var = "python CompleteMacs.py %s %s" % (new_execute, csv_information)
	   	 				var2 = "execute " + var
	   	 				syslog.syslog(syslog.LOG_INFO, var2)
	   					os.system(var)

	   					#chequeamos si termino de manera correcta e imprimimos la cosa...
	   			 		if (CheckExecuteNextStep("FilesOutput/CompleteMacs.txt")==1):
	   			 			#borramos el archivo creado...
	   			 			#os.system("rm FilesOutput/CompleteMacs.txt")
	 	 			 		var = "python RemoveElementsConflictiv.py %s" % new_execute
	 		 		 		var2 = "execute " + var
	 		 		 		syslog.syslog(syslog.LOG_INFO, var2)
	 		 		 		os.system(var)

	 	 			 		if (CheckExecuteNextStep("FilesOutput/Remove.txt")==1):
	 	 			 		 	#borramos el archivo creado...
	   			 	 			#os.system("rm FilesOutput/Remove.txt")
	 	 			 		 	var = "python EstadisticasFinales.py %s" % new_execute
	 	 			 		 	var2 = "excute" + var
	 	 			 		 	syslog.syslog(syslog.LOG_INFO, var2)
	 	 			 		 	os.system(var)

	 	 			 		 	if (CheckExecuteNextStep("FilesOutput/Estadisticas_Error.txt")==1):
	 	 			 		 		os.system("rm FilesOutput/Estadisticas_Error.txt")
	 	 			 		 		syslog.syslog(syslog.LOG_INFO, "Migracion completada de manera satisfactoria")
	 	 			 		 	else:
	 	 			 		 		sys.exit("No es posible continuar con la ejecucion del pipe line dado errores en:\nEstadisticasFinales.py")				
	 	 			 		else:
	 	 			 		 	sys.exit("No es posible continuar con la ejecucion del pipe line dado errores en:\nRemoveElementsConflictiv.py")			
	 					else:
	 					 	sys.exit("No es posible continuar con la ejecucion del pipe line dado errores en:\nCompleteMacs.py")		
	 				else:
	 					sys.exit("No es posible continuar con la ejecucion del pipe line dado errores en:\nCompleteEquipos.py")
	 			else:
					sys.exit("No es posible continuar con la ejecucion del pipe line dado errores en:\nCompleteUsuarios.py")											 			
	   		else:
	   			sys.exit("No es posible continuar con la ejecucion del pipe line dado errores en:\nCompleteTables.py")		
	   	else:
	   		sys.exit("No es posible continuar con la ejecucion del pipe line dado errores en:\nGetMacs.py")	
	else:
	   	sys.exit("No es posible continuar con la ejecucion del pipe line dado errores en:\nVerificaRequisitos.py")					
	
	#se ejecuta el script que permite obtener la informacion que existe en la base de datos y no en la del csv
	var = "python GenerateListUser.py %s %s %s %s %s" % (csv_information, information_dataexport[1][0], information_dataexport[1][1], information_dataexport[1][2], information_dataexport[1][3])
	var2 = "excute " + var
	syslog.syslog(syslog.LOG_INFO, var2)
	os.system(var)
	
	hora_final = time.strftime("%c")
	estadisticas_finales = open("FilesOutput/Estadisticas_Finales.txt", 'a')
	syslog.openlog("HandlerMigrateData.py", syslog.LOG_USER)#abrimos syslog
	var = "Finalizacion de script: "+hora_final
	syslog.syslog(syslog.LOG_INFO,var)
	estadisticas_finales.write(var+"\n")
	estadisticas_finales.close()
	
	#determinamos si el direccion de las estadisticas fue a un archivo o no...
	if args.resumen == 0:#no se redirecciona...
		os.system("rm FilesOutput/Estadisticas_Finales.txt")#borramos el archivo...

 	#se comienza a descartar los archivos que el usuario selecciono...
	for i in list_files_descartados:
		var = "rm FilesOutput/"+i[0]
		print var
		#borrar los archivos que el usuario descarto...
		os.system(var)

	syslog.closelog()
	return 0

if __name__ == '__main__':#llamada a la funcion principal
	main()
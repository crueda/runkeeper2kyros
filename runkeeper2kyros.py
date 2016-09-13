#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2016-09-12
# mail: carlos.rueda@deimos-space.com
# version: 1.0

########################################################################
# version 1.0 release notes:
# Initial version
########################################################################

from __future__ import division
import time
import datetime
import os
import sys
import calendar
import logging, logging.handlers
import json  
import socket 
from haversine import haversine
from threading import Thread
import MySQLdb
import requests

########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./runkeeper2kyros.properties')

LOG = config['directory_logs'] + "/runkeeper2kyros.log"
LOG_FOR_ROTATE = 10

BBDD_HOST = config['BBDD_host']
BBDD_PORT = config['BBDD_port']
BBDD_USERNAME = config['BBDD_username']
BBDD_PASSWORD = config['BBDD_password']
BBDD_NAME = config['BBDD_name']

KCS_HOST = config['KCS_HOST']
KCS_PORT = config['KCS_PORT']

RUNKEEPER_AUTHORIZATION = config['authorization']
RUNKEEPER_URL_FEED = config['url_feed']
RUNKEEPER_ACCEPT_FEED = config['accept_feed']
RUNKEEPER_ACCEPT_ACTIVITY = config['accept_activity']

DEFAULT_SLEEP_TIME = float(config['sleep_time'])
KCS_SLEEP_TIME = float(config['kcs_sleep_time'])

########################################################################

# Se definen los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('runkeeper2kyros')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.INFO)
except:
    print '------------------------------------------------------------------'
    print '[ERROR] Error writing log at %s' % LOG
    print '[ERROR] Please verify path folder exits and write permissions'
    print '------------------------------------------------------------------'
    exit()

########################################################################


########################################################################
# Definicion de funciones
#
########################################################################

def send2kcs(body):
	connectionRetry = 0.5
	try:
		socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		socketKCS.connect((KCS_HOST, int(KCS_PORT)))
		connectedKCS = True
		socketKCS.send(body + '\r\n')
		logger.debug ("Sent to KCS: %s " % body)
		sendMessage = True
		socketKCS.close()
		time.sleep(KCS_SLEEP_TIME)
	except socket.error,v:
		logger.error('Error sending data: %s', v[0])
		try:
			socketKCS.close()
			logger.info('Trying close connection...')
		except Exception, error:
			logger.info('Error closing connection: %s', error)
			pass
			while sendMessage==False:
				try:
					logger.info('Trying reconnection to KCS...')
					socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					socketKCS.connect((KCS_HOST, int(KCS_PORT)))
					connectedKCS = True
					socketKCS.send(body + '\r\n')
					logger.info ("Sent to KCS: %s " % body)
					sendMessage = True
					socketKCS.close()
				except Exception, error:
					logger.info('Reconnection to KCS failed....waiting %d seconds to retry.' , connectionRetry)
					sendMessage=False
					try:
						socketKCS.close()
					except:
						pass
					time.sleep(connectionRetry)

def getRunkeeperKyrosData():
	try:
		dbConnection = MySQLdb.connect(BBDD_HOST, BBDD_USERNAME, BBDD_PASSWORD, BBDD_NAME)

		cursor = dbConnection.cursor()
		queryRunkeepeer = """SELECT DEVICE_ID, 
			AUTHORIZATION, TYPE_ACTIVITY, LAST_ACTIVITY_ID FROM RUNKEEPER"""
		cursor.execute(queryRunkeepeer)
		result = cursor.fetchall()
		cursor.close
		dbConnection.close
	
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )

def getImei(deviceId):
	try:
		dbConnection = MySQLdb.connect(BBDD_HOST, BBDD_USERNAME, BBDD_PASSWORD, BBDD_NAME)
		cursor = dbConnection.cursor()
		query = """SELECT IMEI FROM OBT where DEVICE_ID=xxx"""
		queryImei = query.replace('xxx', str(deviceId))
		cursor.execute(queryImei)
		result = cursor.fetchone()
		cursor.close
		dbConnection.close
	
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )


def updateLastActivityId(deviceId, activityId):
	try:
		dbConnection = MySQLdb.connect(BBDD_HOST, BBDD_USERNAME, BBDD_PASSWORD, BBDD_NAME)
		cursor = dbConnection.cursor()
		query = """UPDATE RUNKEEPER set LAST_ACTIVITY_ID=yyy where DEVICE_ID=xxx"""
		queryRunkeeper = query.replace('xxx', str(deviceId)).replace('yyy', str(activityId))
		cursor.execute(queryRunkeeper)
		dbConnection.commit()
		cursor.close
		dbConnection.close
	
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )


def processActivity(authorization, imei, activityId):
	logger.info ("process runkeeper activity: %s " % str(activityId))
	headers = {"Content-type": "application/x-www-form-urlencoded", "Host": "api.runkeeper.com", "Accept": RUNKEEPER_ACCEPT_ACTIVITY, "Authorization": "Bearer " + authorization}	
	try:
		response = requests.get(RUNKEEPER_URL_FEED + "/" + str(activityId), headers=headers, verify=False, timeout=2)
		if (response.status_code == 200):
			# recorrer el json de respuesta 
			activity = json.loads(response.content)
			str_start_time = activity['start_time']
			start_time = (time.mktime(time.strptime(str_start_time, '%a, %d %b %Y %H:%M:%S'))*1000)
			#print start_time
			activity_path = activity['path']
			activity_distance = activity['distance']
			#lon_anterior, lat_anterior, epoch_anterior = 0,0,0
			timestamp_anterior, metros_anterior = 0,0
			for index in range(len(activity_path)):
				altitude = int(activity_path[index]['altitude'])
				latitude = activity_path[index]['latitude']
				longitude = activity_path[index]['longitude']
				timestamp = activity_path[index]['timestamp']
				epoch_date = start_time + (int(timestamp*1000))	
				#print epoch_date			
				speed = 0
				if (timestamp_anterior!=0):
					metros = activity_distance[index]['distance']
					distancia = metros - metros_anterior
					segundos = timestamp - timestamp_anterior
					speed = (distancia/segundos)*3.6
					#print speed
					metros_anterior = metros

				'''
				if (lon_anterior!=0):
					segundos = (epoch_date - epoch_anterior)/1000
					pos_anterior = (lat_anterior, lon_anterior)
					pos_actual = (latitude, longitude)
					metros = haversine(pos_anterior, pos_actual) * 1000
					velocidad = (metros/segundos)*3.6
					#print velocidad
				'''
				timestamp_anterior = timestamp
				heading = 0
				#lon_anterior = longitude
				#lat_anterior = latitude
				#epoch_anterior = epoch_date

				#trama_gprmd= GPRMC,113548.000,A,4020.1086,N,00340.2196,W,44.39,257.57,130916,,
				s = epoch_date / 1000.0
				pos_date = datetime.datetime.fromtimestamp(s).strftime('%Y%m%d%H%M%S')
				trama_kcs = str(imei) + ',' + str(pos_date) + ',' + str(longitude) + ',' + str(latitude) + ',' + str(speed) + ',' + str(heading) + ',' + str(altitude) + ',9,2,0.0,0.9,3836'
				send2kcs (trama_kcs)
			return True
		else:
			logger.debug("Codigo de error al recuperar los datos de la actividad: " + str(response.status_code))
			return False
	except Exception,e:
		print str(e)
		logger.debug("Error al al recuperar los datos de la actividad")
	return False

def processNewActivities(authorization, deviceId, imei, typeActivity, lastActivityId):
	logger.info ("processNewActivities. DEVICE_ID: %s " % str(deviceId))
	headers = {"Content-type": "application/x-www-form-urlencoded", "Host": "api.runkeeper.com", "Accept": RUNKEEPER_ACCEPT_FEED, "Authorization": "Bearer " + authorization}	
	try:
		response = requests.get(RUNKEEPER_URL_FEED, headers=headers, verify=False, timeout=2)
		if (response.status_code == 200):
			# recorrer el json de respuesta empezando por la mas antigua
			feed = json.loads(response.content)
			feed_activities = feed['items']
			
			for index in reversed(xrange(len(feed_activities))):
				activity = feed_activities[index]
				if (activity['type'] == typeActivity):
					uri = activity['uri']
					activityId = uri[19:len(uri)]
					if (int(activityId) > int(lastActivityId)):
						processActivity (authorization, imei, activityId)
						updateLastActivityId(deviceId, activityId)
			
			return True
		else:
			logger.debug("Codigo de error al recuperar el feed de actividades: " + str(response.status_code))
			return False
	except Exception,e:
		print str(e)
		logger.debug("Error al al recuperar el feed de actividades")
	return False

########################################################################
# Funcion principal
#
########################################################################

def main():
	while True:
		time.sleep(DEFAULT_SLEEP_TIME)

if __name__ == '__main__':
    #main()
    #processActivity(RUNKEEPER_AUTHORIZATION, 109997775551, 862297072)
    
    runkeeperKyros = getRunkeeperKyrosData()
    for data in runkeeperKyros:
    	deviceId = data[0]
    	authorization = data[1]
    	typeActivity = data[2]
    	lastActivityId = data[3]
    	result = getImei(deviceId)
    	imei = result[0]
    	
    	processNewActivities(authorization, deviceId, imei, typeActivity, lastActivityId)
    
    	

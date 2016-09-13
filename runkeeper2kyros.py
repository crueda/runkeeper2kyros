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

from threading import Thread
import MySQLdb as mdb
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

RUNKEEPER_AUTHORIZATION = config['authorization']
RUNKEEPER_URL_FEED = config['url_feed']
RUNKEEPER_ACCEPT_FEED = config['accept_feed']
RUNKEEPER_ACCEPT_ACTIVITY = config['accept_activity']

DEFAULT_SLEEP_TIME = float(config['sleep_time'])

########################################################################

# Se definen los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('runkeeper2kyros')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
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

def processActivity(activityId):
	headers = {"Content-type": "application/x-www-form-urlencoded", "Host": "api.runkeeper.com", "Accept": RUNKEEPER_ACCEPT_ACTIVITY, "Authorization": "Bearer " + RUNKEEPER_AUTHORIZATION}	
	try:
		response = requests.get(RUNKEEPER_URL_FEED + "/" + str(activityId), headers=headers, verify=False, timeout=2)
		print "code:"+ str(response.status_code)
		if (response.status_code == 200):
			# recorrer el json de respuesta 
			activity = json.loads(response.content)
			activity_path = activity['path']
			for index in range(len(activity_path)):
				altitude = activity_path[index]['altitude']
				print altitude
			return True
		else:
			logger.debug("Codigo de error al recuperar los datos de la actividad: " + str(response.status_code))
			return False
	except Exception,e:
		print str(e)
		logger.debug("Error al al recuperar los datos de la actividad")
	return False

def processNewActivities(typeActivity, lastActivityId):
	headers = {"Content-type": "application/x-www-form-urlencoded", "Host": "api.runkeeper.com", "Accept": RUNKEEPER_ACCEPT_FEED, "Authorization": "Bearer " + RUNKEEPER_AUTHORIZATION}	
	try:
		response = requests.get(RUNKEEPER_URL_FEED, headers=headers, verify=False, timeout=2)
		if (response.status_code == 200):
			# recorrer el json de respuesta empezando por la mas antigua
			feed = json.loads(response.content)
			feed_activities = feed['items']
			'''
			for index in reversed(xrange(len(feed_activities))):
				activity = feed_activities[index]
				if (activity['type'] == typeActivity):
					uri = activity['uri']
					activityId = uri[19:len(uri)]
					processActivity (activityId)
			'''
			processActivity(862297072)
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
	processNewActivities('Cycling', 0)

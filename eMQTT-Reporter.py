import logging
import os
import mysql.connector
import csv
import schedule
import time
from dotenv import load_dotenv
load_dotenv()
#logging.basicConfig(encoding='utf-8', level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(filename='logs/eMQTT-Reporter.log', encoding='utf-8', level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')


mydb = mysql.connector.connect(
  host=os.getenv('DATABASE_IP'),
  user=os.getenv('DATABASE_USER'),
  password=os.getenv('DATABASE_PASSWORD'),
  database=os.getenv('DATABASE_NAME')
)
list_simulations = []
def CreateDataSetToCsv (tokens_sensor,dataset_name):
  data = []
  header = ['tcp_flags','tcp_time_delta','tcp_len','mqtt_conack_flags','mqtt_conack_flags_rserved','mqtt_conack_flags_sp','mqtt_conack_val','mqtt_conflag_cleansess','mqtt_conflag_passwd','mqtt_conflag_qos','mqtt_conflag_reserved','mqtt_conflag_retain','mqtt_conflag_uname','mqtt_conflag_willflag','mqtt_conflags','mqtt_dupflag','mqtt_hdrflags','mqtt_kalive','mqtt_len','mqtt_msg','mqtt_msgid','mqtt_msgtype','mqtt_proto_len','mqtt_protoname','mqtt_qos','mqtt_retain','mqtt_sub_qos','mqtt_suback_qos','mqtt_ver','mqtt_willmsg','mqtt_willmsg_len','mqtt_willtopic','mqtt_willtopic_len']
  for sensors in range(len(tokens_sensor)):
      mycursor = mydb.cursor()
      arg=[tokens_sensor[sensors]]
      mycursor.callproc('mqtt_create_dataset', arg)
      for result in mycursor.stored_results():
        data.append(result.fetchall())
  with open(os.path.join('/eMQTT/eMQTT-FrontEnd/public/results/',dataset_name+'.csv') , 'w', encoding='UTF8', newline='') as f:
      writer = csv.writer(f)
      writer.writerow(header)
      for line in data:
          logging.debug(line)
          writer.writerows(line)          
          writer.writerows('\n')
def RecoverSimulationsAndSensor(list_simulations):
  for results in list_simulations:
    logging.info("Getting sensors from: "+results)
    mycursor = mydb.cursor()
    mycursor.execute("UPDATE sensor INNER JOIN execution ON sensor.id = execution.sensor_id INNER JOIN simulation ON simulation.id = execution.execution SET sensor.status = 0, sensor.running = 0 WHERE simulation.token = %s", (results,))
    mydb.commit()
    logging.info(str(mycursor.rowcount)+"record(s) affected by the UPDATE to turn off the sensors")
    mycursor.execute("SELECT sensor.token FROM sensor INNER JOIN execution ON sensor.id = execution.sensor_id INNER JOIN simulation ON simulation.id = execution.execution WHERE sensor.status=0 and sensor.running = 0 and simulation.token =%s", (results,)) 
    list_sensors= [list_sensors[0] for list_sensors in mycursor.fetchall()]
    logging.info(list_sensors)
    CreateDataSetToCsv(list_sensors,results)
    logging.info(results)
    mycursor.execute("UPDATE simulation SET dataset = 1 WHERE simulation.token =%s", (results,)) 
    mydb.commit()
    logging.info(str(mycursor.rowcount)+"record(s) affected by the UPDATE to complete the cycle")
    mycursor.close()

logging.info("Executing by schedule")
mycursor = mydb.cursor()
mycursor.execute("SELECT token FROM `simulation` WHERE status = 0 AND dataset IS NULL;")
list_simulations= [list_simulations[0] for list_simulations in mycursor.fetchall()]
logging.debug(list_simulations)
RecoverSimulationsAndSensor(list_simulations)
mycursor.close()
mydb.close()
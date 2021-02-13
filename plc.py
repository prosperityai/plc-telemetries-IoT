

# This  script connects to Modbus slave device to read  
# a telemetries and publish to an MQTT Topic in AWS IoT every 5 seconds.
# If an exception occurs, it will wait 5 seconds and try again.
# Since the function is long-lived it will run forever when deployed to a 
# Greengrass core.  

import greengrasssdk
import platform
from threading import Timer
import time
import logging
import sys
import json
# import pymodbus libraries for the modbus client
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.constants import Endian
from pymodbus.compat import iteritems

# Instantiate the client for your modbus slave device. Change this to your 
# desired IP and Port.
mbclient = ModbusClient('127.0.0.1', port=5020)
#20001

# avoid root permissions.
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# Creating a greengrass core sdk client
client = greengrasssdk.client('iot-data')

# in an infinite loop, this procedure will poll the telemetries from a
# modbus slave device  and publish the value to AWS IoT via MQTT.
def poll_telemetries():
    while True:
        try:
            # connect to modbus slave device
            mbclient.connect()
            # set the address and number of bytes that will be read on the modbus device
            address = 0x00
            count = 8
            # read the holding register value for the telemtries
            rr = mbclient.read_holding_registers(address, count, unit=1)
            # decode results as a 32 bit float
            decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, endian=Endian.Big)
            decoded = {
                'float': decoder.decode_32bit_float()
            }
            # publish results to topic in AWS IoT
            for name, value in iteritems(decoded):
                client.publish(topic='dt/plc1/telemetry', payload=json.dumps({ 'message': value}))
        except Exception as e:
            logging.info("Error: {0}".format(str(e)))

        time.sleep(5)

poll_telemetries()

def function_handler(event, context):
    return
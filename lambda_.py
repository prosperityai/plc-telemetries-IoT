import json
import boto3

iot_core = boto3.client('iot-data')


#======================================================================
#============ LAMBDA MAIN FUNCTION LISTENIG TO AN EVENT ===============
#======================================================================

def lambda_handler(event, context):
        if not isinstance(event, dict):
            return {
                'status_code': 400, 
                'body':json.dumps('Message must be a string')
            }
        
        if not event.get('plc_string'):
            return {
                'status_code': 400, 
                'body':json.dumps('Json must consist of key plc_string')
            }
            
        data_string = event.get('plc_string')
        parsed_json = parse(data_string)
        if parsed_json.get('response'):
            return {
                'status_code':400,
                'body':json.dumps(parsed_json)
                
            }
        
        publish('plc-data', parsed_json)
        return {
            'status_code':200, 
            'body': json.dumps('Data have been published successfully')
        }

#===============================================================
#========== FUCTION TO PUBLISH DATA TO THE IOT CORE ============
#===============================================================

def publish(topic:str, payload:dict):
    iot_core.publish(
        topic = topic, 
        qos = 0, 
        payload =json.dumps(payload)
        )


#=================================================================
#========== FUNCTION TO PARSE A STRING TO A JSON FORMAT ==========
#=================================================================

def parse(plc_string: str) -> dict:
    if not isinstance(plc_string, str):
        return {
            'response': 'Failed to parse plc message it must be a string'
        }
    if len(plc_string) != 90:
        return {
            'response': 'Failed to parse plc message it must have a length of 90 charachers'
        }

    return {
        'Header': {
            'Identification': plc_string[0:2],
            'MessageNumber': plc_string[2:4],
            'Error': plc_string[4:6],
            'MessageSource': plc_string[6:10],
            'MessageDestination': plc_string[10:14]
        },
        'UserProcessedData': {
            'Zone': plc_string[14:18],
            'FromPosition': plc_string[18:22],
            'ToPosition': plc_string[22:26],
            'Code': plc_string[26:30],
            'Pk1': plc_string[30:34],
            'Pk2': plc_string[34:38],
            'TriggerIdentification': plc_string[38:40],
            'FromAisleCoordinate': plc_string[40:43],
            'FromXCoordinate': plc_string[43:46],
            'FromYCoordinate': plc_string[46:49],
            'FromZCoordinate': plc_string[49:52],
            'ToAisleCoordinate': plc_string[52:55],
            'ToXCoordinate': plc_string[55:58],
            'ToYCoordinate': plc_string[58:61],
            'ToZCoordinate': plc_string[61:64],
            'Info1': plc_string[64:70],
            'Info2': plc_string[70:90]
        }
    }

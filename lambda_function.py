import json
import base64
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    # logger.debug(f"event: {json.dumps(event)}")
    logger.debug(f"records: {len(event['records'])}")
    records = []
    for encoded_record in event['records']:
        record_data = json.loads(base64.b64decode(encoded_record['data']))
        summary = {
            "email": record_data['email'],
            "first": record_data['name']['first'],
            "last": record_data['name']['last'],
            "age": record_data['dob']['age'],
            "gender": record_data['gender'],
            "country": record_data['location']['country'],
            "latitude": record_data['location']['coordinates']['latitude'],
            "longitude": record_data['location']['coordinates']['longitude'],
        }
        data = {
            "original_record": record_data,
            "summary": summary
        }
        logger.info(f"{json.dumps(data)}")
        record = {
            "recordId": encoded_record['recordId'],
            "result": "Ok",
            "data": base64.b64encode(json.dumps(data).encode('utf-8')).decode('utf-8')
        }
        records.append(record)
    logger.info(f"records parsed: {len(records)}")

    return {"records": records}

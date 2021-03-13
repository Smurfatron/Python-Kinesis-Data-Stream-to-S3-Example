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
        record_data = base64.b64decode(encoded_record['data'])
        record = {
            "id": encoded_record['recordId'],
            "data": json.loads(record_data)
        }
        records.append(record)
        logger.info(f"{json.dumps(record)}")
    logger.info(f"records parsed: {len(records)}")

    return records
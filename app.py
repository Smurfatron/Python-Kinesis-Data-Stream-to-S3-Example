import urllib3
import json
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_data(count):
    """
    Get a bunch of randomly generated user data using https://randomuser.me/
    :return: randomly generated user data
    """
    url = 'https://randomuser.me/api/'

    # csv, json, prettyjson, yaml, xml
    format = 'json'
    # nationalities AU, BR, CA, CH, DE, DK, ES, FI, FR, GB, IE, IR, NO, NL, NZ, TR, US
    nat = ['us', 'gb', 'es', 'de']
    results = count

    query_params = {
        "format": format,
        "nat": ','.join(nat),
        "results": results
    }

    http = urllib3.PoolManager()
    try:
        r = http.request('GET', url, fields=query_params)
        if r.status != 200:
            logger.error(f"Exception trying to get randomly generated user record: {r.status}, {r.data}")
            return None
        else:
            data = json.loads(r.data)['results']
            return data
    except Exception as ex:
        logger.error(f"Exception trying to get randomly generated user record: {ex}")


def generate(count, stream_name, kinesis_client):
    """
    Create a Kinesis data stream and put the user data in it
    :param stream_name:
    :param kinesis_client:
    :return:
    """
    kstream = KinesisStream(kinesis_client)
    try:
        print(kstream.describe(stream_name))
    except Exception as ex:
        logger.warning(f"Exception trying to describe {stream_name}, will try to create the stream")
        kstream.create(stream_name, wait_until_exists=True)
    finally:
        logger.debug(f"kstream.arn(): {kstream.arn()}")
        #while True:
        data = get_data(count)
        for user in data:
            # logger.info(f"{json.dumps(user)}")
            logger.debug(f"Putting user data for: {user['name']['first']} {user['name']['last']}")
            '''
            {"gender": "male", "name": {"title": "Mr", "first": "Vincent", "last": "Hanson"}, "location": {"street": {"number": 9132, "name": "Walnut Hill Ln"}, "city": "Gainesville", "state": "Nevada", "country": "United States", "postcode": 57555, "coordinates": {"latitude": "-89.0774", "longitude": "-162.9790"}, "timezone": {"offset": "-11:00", "description": "Midway Island, Samoa"}}, "email": "vincent.hanson@example.com", "login": {"uuid": "f50ef588-7600-4190-95da-64846194d369", "username": "ticklishmouse455", "password": "franco", "salt": "BBdahMbL", "md5": "1be1a234fb938364f360577af21bc0af", "sha1": "8d9883139948df2315ba87547581e4bcf346b17d", "sha256": "5cdb5f612ceeda63adf137e24ef0c482e71b3c5cd08e71fe23194e48d8064f38"}, "dob": {"date": "1961-04-03T16:38:27.988Z", "age": 60}, "registered": {"date": "2011-03-29T04:23:09.542Z", "age": 10}, "phone": "(405)-382-4536", "cell": "(294)-051-3770", "id": {"name": "SSN", "value": "317-78-6541"}, "picture": {"large": "https://randomuser.me/api/portraits/men/25.jpg", "medium": "https://randomuser.me/api/portraits/med/men/25.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/men/25.jpg"}, "nat": "US"}
            '''
            try:
                kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(user),
                    PartitionKey="partitionkey")
            except Exception as ex:
                logger.error(f"Exception trying to put record to Kinesis data stream {stream_name}: {ex}")
                break


if __name__ == '__main__':
    import boto3
    from KinesisStream import KinesisStream
    stream_name = 'kds-ue1-dev-kinesis-test'
    count = 100
    generate(count, stream_name, boto3.client('kinesis'))

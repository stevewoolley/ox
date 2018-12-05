from flask import Flask, Response, request, jsonify, render_template
import requests
import boto3
import os
import simplejson as json
from boto3.dynamodb.conditions import Key, Attr

app = Flask(__name__)

# OpenWeatherMap
WEATHER_URL = 'http://api.openweathermap.org/data/2.5/weather?'
WEATHER_UNIT = os.environ.get('weather_unit')
WEATHER_HOME_CITY = os.environ.get('weather_home_city')
WEATHER_API_KEY = os.environ.get('weather_api_key')

AWS_REGION = os.environ.get('aws_region')
SNAPSHOT_BUCKET = os.environ.get('snapshot_bucket')
ARCHIVE_BUCKET = os.environ.get('archive_bucket')

SENSORS_TABLE = 'sensors'
SENSORS_PARTITION_KEY = 'source'
SENSORS_SORT_KEY = 'timestamp'
SENSORS_PAYLOAD = 'payload'
PAYLOAD_PREFIX = 'payload.state.reported.{}'
SCAN_INDEX_FORWARD = False
ITEMS = 'Items'


def bucket_browser(bucket):
    s3 = boto3.client('s3')
    results = []
    s3_args = {'Bucket': bucket}
    if 'prefix' in request.args:
        s3_args['Prefix'] = request.args['prefix']
    while True:
        k_resp = s3.list_objects_v2(**s3_args)
        try:
            contents = k_resp['Contents']
        except KeyError:
            return
        for obj in contents:
            o = dict()
            o['name'] = obj['Key']
            o['url'] = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
            o['timestamp'] = obj['LastModified']
            o['size'] = obj['Size']
            o['etag'] = obj['ETag'].strip('\"')
            resp = s3.get_object_tagging(Bucket=bucket, Key=obj['Key'])
            if 'TagSet' in resp:
                o['tags'] = resp['TagSet']
            results.append(o)
        try:
            s3_args['ContinuationToken'] = k_resp['NextContinuationToken']
        except KeyError:
            break
    return jsonify(results)


@app.route('/weather', methods=['GET'])
def weather():
    # OpenWeatherMap API https://openweathermap.org
    where = "&q={}".format(WEATHER_HOME_CITY)
    if 'lat' in request.args and 'lon' in request.args:
        where = '&lat={}&lon={}'.format(request.args['lat'], request.args['lon'])
    elif 'id' in request.args:
        where = '&id={}'.format(request.args['id'])
    elif 'q' in request.args:
        where = '&q={}'.format(request.args['q'])
    url = "{}appid={}&units={}{}".format(WEATHER_URL, WEATHER_API_KEY, WEATHER_UNIT, where)
    return jsonify(requests.get(url).json())


@app.route('/movies', methods=['GET'])
def movies():
    ddb = boto3.resource('dynamodb', region_name=AWS_REGION)
    return jsonify(sorted(ddb.Table('movies').scan()['Items'], key=lambda k: k['year']))


@app.route('/triggers', methods=['GET'])
def triggers():
    ddb = boto3.resource('dynamodb', region_name=AWS_REGION)
    return jsonify(sorted(ddb.Table('triggers').scan()['Items'], key=lambda k: k['idx']))


@app.route('/things', methods=['GET'])
def things():
    iot = boto3.client('iot', region_name=AWS_REGION)
    return jsonify(sorted(iot.list_things()["things"], key=lambda k: k['thingName']))


@app.route('/things/<thing_id>', methods=['GET'])
def thing(thing_id):
    iot_data = boto3.client('iot-data', region_name=AWS_REGION)
    resp = iot_data.get_thing_shadow(thingName=thing_id)
    body = resp['payload']
    return jsonify(json.loads(body.read()))


@app.route('/things/<thing_id>/<metric>', methods=['GET'])
def thing_metric(thing_id, metric):
    iot_data = boto3.client('iot-data', region_name=AWS_REGION)
    resp = iot_data.get_thing_shadow(thingName=thing_id)
    body = resp['payload']
    j = json.loads(body.read())
    if 'state' in j:
        if 'reported' in j['state']:
            if metric in j['state']['reported']:
                return jsonify(j['state']['reported'][metric])
    return Response(status=404)


@app.route('/things/<thing_id>/<metric>/history', methods=['GET'])
def thing_metric_history(thing_id, metric):
    DDB = boto3.resource('dynamodb', region_name=AWS_REGION)
    p_key = Key(SENSORS_PARTITION_KEY).eq(thing_id)  # query must be partitioned
    ex = {"#pkey": SENSORS_PARTITION_KEY, "#skey": SENSORS_SORT_KEY}
    pe = "#pkey, #skey, {}"
    item = []
    for idx, a in enumerate(PAYLOAD_PREFIX.format(metric).split('.')):
        ex["#n{}".format(idx)] = a
        item.append("#n{}".format(idx))
    resp = DDB.Table(SENSORS_TABLE).query(
        ScanIndexForward=SCAN_INDEX_FORWARD,
        KeyConditionExpression=p_key,
        FilterExpression=Attr(PAYLOAD_PREFIX.format(metric)).exists(),
        ExpressionAttributeNames=ex,
        ProjectionExpression=pe.format('.'.join(item))
    )
    if ITEMS in resp:
        # transform result to extract only interesting json key
        for item in resp[ITEMS]:
            tmp = item
            if PAYLOAD_PREFIX.format(metric) is not None:
                for i in PAYLOAD_PREFIX.format(metric).split('.'):
                    tmp = tmp.get(i)
                item[PAYLOAD_PREFIX.format(metric).split('.')[-1]] = tmp
            del item[SENSORS_PAYLOAD]
        #
        return jsonify(resp[ITEMS])
    else:
        return Response(status=404)


@app.route('/snapshots', methods=['GET'])
def snapshots():
    return bucket_browser(SNAPSHOT_BUCKET)


@app.route('/archive', methods=['GET'])
def archive():
    return bucket_browser(ARCHIVE_BUCKET)


@app.route('/publish/<path:topic>', methods=['GET'])
def publish(topic):
    payload = {}
    if 'payload' in request.args:
        payload = request.args['payload']
    qos = 0
    if 'qos' in request.args:
        payload = int(request.args['qos'])
    iot_data = boto3.client('iot-data', region_name=AWS_REGION)
    iot_data.publish(topic='{}'.format(topic), qos=qos, payload=json.dumps(payload))
    return Response(status=200)


if __name__ == '__main__':
    app.run()

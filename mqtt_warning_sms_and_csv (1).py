import csv
import json
import os
import shutil
import time
from datetime import datetime
import requests
import paho.mqtt.client as mqtt
MQTT_BROKER = 'broker.hivemq.com'
MQTT_PORT = 1883
MQTT_TOPIC = 'student-demo-982734/wemos1/sensors'
API_KEY = 'cd_sou_050526_5TnNFA'
PHONE_NUMBER = '918250053100'
TEMPLATE_ID = '113'
SMS_COOLDOWN_SECONDS = 60
last_sms_time = 0
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'mqtt_logs')
CSV_PREFIX = 'mqtt_sensor_log'
CSV_COLUMNS = ['time', 'device', 'temperature', 'humidity', 'pressure', 'mq02', 'load_temperature', 'vibration', 'warning', 'emergency', 'reason', 'sample', 'nano_millis', 'wemos_millis', 'sms_status']

def clean_text(text):
    text = str(text).replace(' ', '_')
    return text[:30]

def get_csv_path(now_dt=None):
    if now_dt is None:
        now_dt = datetime.now()
    date_text = now_dt.strftime('%Y-%m-%d')
    return os.path.join(LOG_DIR, f'{CSV_PREFIX}_{date_text}.csv')

def read_existing_header(csv_path):
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as csv_file:
            first_line = csv_file.readline().strip()
        if not first_line:
            return []
        return next(csv.reader([first_line]))
    except Exception:
        return []

def ensure_csv_file(now_dt=None):
    if now_dt is None:
        now_dt = datetime.now()
    os.makedirs(LOG_DIR, exist_ok=True)
    csv_path = get_csv_path(now_dt)
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        existing_header = read_existing_header(csv_path)
        if existing_header and existing_header != CSV_COLUMNS:
            backup_stamp = datetime.now().strftime('%H%M%S')
            backup_path = csv_path.replace('.csv', f'_old_format_backup_{backup_stamp}.csv')
            shutil.move(csv_path, backup_path)
            print(f'[CSV] Old-format file backed up | {backup_path}')
    file_exists = os.path.exists(csv_path)
    write_header = not file_exists or os.path.getsize(csv_path) == 0
    if write_header:
        with open(csv_path, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
            writer.writeheader()
        print(f'[CSV] Created lightweight file | {csv_path}')
    return csv_path

def append_csv_row(row):
    now_dt = datetime.now()
    csv_path = ensure_csv_file(now_dt)
    row['time'] = now_dt.strftime('%H:%M:%S')
    with open(csv_path, 'a', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS, extrasaction='ignore')
        writer.writerow(row)
        csv_file.flush()
    return csv_path

def log_mqtt_data(data, emergency, reason, sms_status):
    row = {'device': data.get('device', ''), 'temperature': data.get('temperature', ''), 'humidity': data.get('humidity', ''), 'pressure': data.get('pressure', ''), 'mq02': data.get('mq02', ''), 'load_temperature': data.get('load_temperature', ''), 'vibration': data.get('vibration', ''), 'warning': data.get('warning', ''), 'emergency': 1 if emergency else 0, 'reason': reason, 'sample': data.get('sample', ''), 'nano_millis': data.get('nano_millis', ''), 'wemos_millis': data.get('wemos_millis', ''), 'sms_status': sms_status}
    csv_path = append_csv_row(row)
    print(f'[CSV] Logged lightweight row | {csv_path}')

def log_parse_error(error_text):
    row = {'device': '', 'temperature': '', 'humidity': '', 'pressure': '', 'mq02': '', 'load_temperature': '', 'vibration': '', 'warning': '', 'emergency': 0, 'reason': 'Parse_Error', 'sample': '', 'nano_millis': '', 'wemos_millis': '', 'sms_status': f'Parse error: {str(error_text)[:80]}'}
    csv_path = append_csv_row(row)
    print(f'[CSV] Parse error logged without raw payload | {csv_path}')

def send_sms(reason, temp, load_temp, vibration, mq02):
    url = f'https://www.circuitdigest.cloud/api/v1/send_sms?ID={TEMPLATE_ID}'
    headers = {'Authorization': API_KEY, 'Content-Type': 'application/json'}
    payload = {'mobiles': PHONE_NUMBER, 'var1': 'Machine1', 'var2': clean_text(f'{reason}_T{temp}_L{load_temp}_V{vibration}_G{mq02}')}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        print(f'[SMS] {r.status_code} | {r.text}')
        return f'HTTP {r.status_code}'
    except Exception as e:
        print(f'[SMS] Failed | {e}')
        return f'Failed: {e}'

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print('[MQTT] Connected')
        client.subscribe(MQTT_TOPIC)
        print(f'[MQTT] Subscribed | {MQTT_TOPIC}')
    else:
        print(f'[MQTT] Connection failed | Code={rc}')

def on_message(client, userdata, msg):
    global last_sms_time
    try:
        payload_text = msg.payload.decode(errors='replace')
        data = json.loads(payload_text)
        temp = float(data.get('temperature', 0))
        humidity = float(data.get('humidity', 0))
        pressure = float(data.get('pressure', 0))
        mq02 = float(data.get('mq02', 0))
        load_temp = float(data.get('load_temperature', 0))
        vibration = float(data.get('vibration', 0))
        warning = int(data.get('warning', 0))
        emergency = False
        reason = 'Normal'
        sms_status = 'Not required'
        if warning == 1:
            emergency = True
            reason = 'Monitoring_Warning'
        elif temp > 45:
            emergency = True
            reason = 'High_Temperature'
        elif load_temp > 60:
            emergency = True
            reason = 'High_Load_Temp'
        elif vibration > 600:
            emergency = True
            reason = 'High_Vibration'
        elif mq02 > 180:
            emergency = True
            reason = 'High_Gas'
        elif humidity > 90:
            emergency = True
            reason = 'High_Humidity'
        elif pressure < 950 or pressure > 1100:
            emergency = True
            reason = 'Pressure_Warning'
        if emergency:
            print(f'[MQTT] WARNING | {reason} | T={temp:.1f}C | Load={load_temp:.1f}C | Vib={vibration:.0f} | MQ02={mq02:.0f} | Warn={warning}')
            now = time.time()
            if now - last_sms_time >= SMS_COOLDOWN_SECONDS:
                last_sms_time = now
                sms_status = send_sms(reason, temp, load_temp, vibration, mq02)
            else:
                remaining = int(SMS_COOLDOWN_SECONDS - (now - last_sms_time))
                sms_status = f'Cooldown active, wait {remaining}s'
                print(f'[SMS] {sms_status}')
        else:
            print(f'[MQTT] NORMAL  | T={temp:.1f}C | Load={load_temp:.1f}C | Vib={vibration:.0f} | MQ02={mq02:.0f} | Warn={warning}')
        log_mqtt_data(data, emergency, reason, sms_status)
    except Exception as e:
        print(f'[ERROR] {e}')
        log_parse_error(e)
print('================================')
print('MQTT Warning SMS Listener Started')
print('Lightweight datewise CSV logging enabled')
print(f'CSV folder: {LOG_DIR}')
print('CSV file format: mqtt_sensor_log_YYYY-MM-DD.csv')
print('CSV columns: time + processed sensor/status data only')
print('================================')
ensure_csv_file()
while True:
    try:
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except AttributeError:
            client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        print('[MQTT] Connecting...')
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        print('[SYSTEM] Stopped by user')
        break
    except Exception as e:
        print(f'[SYSTEM] Error | {e}')
        print('[SYSTEM] Restarting in 5 seconds...')
        time.sleep(5)

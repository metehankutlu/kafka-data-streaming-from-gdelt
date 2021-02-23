import os
import requests
import zipfile
import io
from pykafka import KafkaClient

def main():
    KAFKA_HOST = 'localhost:9092'
    GDELT_URL = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'

    client = KafkaClient(hosts = KAFKA_HOST)
    topic = client.topics['t1']

    response = requests.get(GDELT_URL)
    data = str(response.content, 'UTF-8').strip()
    current_dir = os.path.dirname(os.path.realpath(__file__))
    tmp_dir = os.path.join(current_dir, 'tmp')

    lines = data.split('\n')
    print('Found %d files\n' % len(lines))

    for line in lines:
        zip_url = line.split(' ')[-1]
        zip_name = zip_url.split('/')[-1]
        print('%s found and downloading...' % zip_name)
        zip_file = requests.get(zip_url)
        # with open(zip_name, 'wb') as f:
        #     f.write(zip_file.content)
        print('Download finished.')
        print('Extracting...')
        with zipfile.ZipFile(io.BytesIO(zip_file.content)) as zip_ref:
            zip_ref.extractall('./tmp')
        csv_name = zip_name.strip('.zip')
        csv_path = os.path.join(tmp_dir, csv_name)
        print('Sending \'%s\' to Kafka.\n' % csv_name)
        with topic.get_sync_producer() as producer:
            producer.produce(bytes(csv_path, encoding='UTF-8'))

if __name__ == '__main__':
    main()
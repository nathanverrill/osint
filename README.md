# OSINT Project

Nathan Verrill

nathan.verrill@gmail.com

## Goal

Demonstrate processing pipeline for realtime analytics, standardization and AI/ML with OSINT data, such as maritime AIS, ADSB, GDELT, and others.

### Usage for AIS

#### start Redpanda

in root of project directory `docker-compose up -d` and confirm running with `docker ps`

#### get AIS Streamio Key from aisstream.io website

update `config.yaml` with Streamio key. See `https://aisstream.io/`

#### Python 3.10 environment

using conda or local 3.10

for conda: `conda create -n osint python=3.10` and `conda activate osint`

#### Load python libraries

`python3 -m pip install -r requirements.txt`

#### Start producer

`python3 ais_streamio_producer.py`

#### View in Redpanda

Navigate to `http://localhost:8080/topics/ais_streamio`

The topic should have been autocreated and you'll see data

the MMSI is the Kafka Key, you may see unable to deserialize the value. this is because the message is in protobuf

TODO: figure out how to deserialize message into protobuf in console

#### View decompressed stream

`python3 ais_streamio_consumer_test.py`

The consumer starts with latest, so you'll only see data if producer is running.

The purpose of the consumer script is to get a quick preview of the data

#### Further optional configuration

In Redpanda console, set retention to infinite and storage to your desired amount, depending on your local, something like 50 GiB

# OSINT Project

Nathan Verrill

nathan.verrill@gmail.com

## Goal

Demonstrate processing pipeline for realtime analytics, standardization and AI/ML with OSINT data, such as maritime AIS, ADSB, GDELT, and others.

### Usage for AIS

#### Start Kafka

Redpanda is used for the Kafka broker. Redpanda is faster and easier to use, and compatible with all Kafka APIs.

For local development, utilize a single node cluster. Navigate to the single_node directory and run `docker-compose up -d` and confirm running with `docker ps` or in Docker Desktop

A docker compose for a three_node broker is also included.

#### get AIS Streamio Key from aisstream.io website

update `config.yaml` with Streamio key. See `https://aisstream.io/`

#### Python 3.10 environment

using conda, pyenv, or local 3.10

for conda: `conda create -n osint python=3.10` and `conda activate osint`

for pyenv: `pyenv virtualenv 3.10 osint` and `pyenv local osint`

#### Load python libraries

`python3 -m pip install -r requirements.txt`

#### Start producer

Script subscribes to AIS Streamio web socket and publishes to raw topic in Kafka.
`python3 aisstreamio_subscriber.py`

#### View in Redpanda

Navigate to `http://127.0.0.1:8080/topics/ais_streamio_reports_raw`

The topic should have been autocreated and you'll see data. Message Type is used for the Kafka key.

#### Run pipeline

The bytewax library is used for stream processing. See bytewax.io.

`python3 -m bytewax.run aisstreamio_pipeline.py`

The pipeline standardizes and enriches each of the raw messages and publishes each message into a topic specific to that message type.

Navigate to Redpanda console to see Kafka topics for each message type.

`http://127.0.0.1:8080/topics`

#### Further optional configuration

In Redpanda console, set retention to infinite and storage to your desired amount, depending on your local, something like 20GB.

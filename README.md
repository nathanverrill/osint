# OSINT Project

Nathan Verrill

nathan.verrill@gmail.com

## Goal

Demonstrate processing pipeline for realtime analytics, standardization and AI/ML with OSINT data, such as maritime AIS, ADSB, GDELT, and others.

### Usage for AIS

If you haven't already, update config.yaml and env variables for your configuration.

update `config.yaml` with Streamio key. See `https://aisstream.io/`

#### Ingestion

`make ingest`

Builds and starts the Redpanda docker containers for processing events and the live map.

To start receiveing AIS data from AIS Streamio, activate the appropriate virtual environment and install `requirements.txt` then run `python3 aisstreamio_subscriber.py`

You can check the Redpanda console to confirm the `ais_streamio_reports_raw` topic is being populated at `http://localhost:8080/topics`

#### Processing

To run enrichment pipeline, setup the local python environment:

`python3 -m pip install -r requirements.txt`

The bytewax library is used for stream processing. See bytewax.io.

`python3 -m bytewax.run aisstreamio_pipeline.py`

The pipeline standardizes and enriches each of the raw messages and publishes each message into a topic specific to that message type.

Due to buffering, it may take a minute before the new topics are populated.

#### Live map

A live map is visible at `http://localhost:8001/map`.

The map is updated when there are new event messages in the `ais_positionreport` topic. A new pin is added when the map sees an MMSI for the first time, and moved if an existing MMSI's position has changed.

#### Further optional configuration

In Redpanda console, set time retention to infinite and storage to your desired amount, depending on your local, something like 20GB.

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

Builds and starts the docker containers to automatically start ingesting from the ais streamio web socket into the `ais_streamio_reports_raw`. You can navigate to `http://localhost:8080/topics` to see the messages.

#### Processing

To run enrichment pipeline, setup the local python environment:

`python3 -m pip install -r requirements.txt`

The bytewax library is used for stream processing. See bytewax.io.

`python3 -m bytewax.run aisstreamio_pipeline.py`

The pipeline standardizes and enriches each of the raw messages and publishes each message into a topic specific to that message type.

Due to buffering, it may take a minute before the new topics are populated.

#### Live map

A live map is visible at `http://localhost:8001/map`.

Due to buffering, it may take a minute or two before pins start to appear on the map.

#### Further optional configuration

In Redpanda console, set time retention to infinite and storage to your desired amount, depending on your local, something like 20GB.

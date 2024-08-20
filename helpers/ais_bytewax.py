import orjson
from flatten_json import flatten
from helpers.binned_location import BinnedLocation
from bytewax.connectors.kafka import KafkaSinkMessage
from uuid import uuid4
from dateutil import parser

class AISBytewaxOperations:
    
    @staticmethod
    def format_timestamp(msg):
        """Make timestamps epoch and python datetime compatible."""
        return KafkaSinkMessage(msg.key, msg.value)    
    
    @staticmethod
    def enrich_metadata(msg):
        """Assign id and standardize MMSI, shipname, and add tracking."""
        msg_json = orjson.loads(msg.value)

        # assign uuid for traceability
        msg_json['UUID'] = uuid4()
        
        # extract nested MetaData
        md = msg_json['MetaData']
        
        # remove trailing UTC so it converts properly
        # example: 2024-08-19 17:46:57.481183349 +0000 UTC
        dt = parser.parse(md['time_utc'][:-4])        
        msg_json['TimestampIso'] = dt.isoformat()
        msg_json['Timestamp'] = dt.timestamp()        
                
        # allow leading zeroes in mmsi
        msg_json['MMSI'] = str(md['MMSI'])
                
        # remove trailing space in ship name
        msg_json['ShipName'] = md['ShipName'].strip()
        
        # move lat/lng to top level
        msg_json['Lat'] = md['latitude']
        msg_json['Lng'] = md['longitude']
        
        # remove nested metadata
        if 'MetaData' in msg_json:
            del msg_json['MetaData']
        
        # return
        return KafkaSinkMessage(msg.key, orjson.dumps(msg_json))    

    @staticmethod
    def bin_location(msg):
        """Optimize location in multiple formats for analytics."""
        msg_json = orjson.loads(msg.value)

        # uber hexes for geo analytics
        # https://h3geo.org/
        # 450m edge hex at res 8    
        h3_resolution = 8

        # googles geo indexing
        # http://s2geometry.io/
        # 610m_sphere    
        s2_level = 14

        # standardize and bin 
        bin = BinnedLocation(msg_json["Lat"], msg_json["Lng"], h3_resolution, s2_level)
        msg_json['LatReported'] = bin.lat_reported
        msg_json['LngReported'] = bin.lng_reported
        msg_json['Lat'] = bin.lat
        msg_json['Lng'] = bin.lng
        msg_json['H3'] = bin.h3
        msg_json['S2'] = bin.s2 
        msg_json['WKT'] = bin.wkt
        msg_json['MGRS'] = bin.mgrs

        return KafkaSinkMessage(msg.key, orjson.dumps(msg_json))


    @staticmethod
    def calculate_features(msg):
        """Extract values useful for analytics and visualization."""
        return KafkaSinkMessage(msg.key, msg.value)
    
    @staticmethod
    def flatten_json(msg):
        """Flatten JSON structure."""
        flat_msg = flatten(orjson.loads(msg.value))
        return KafkaSinkMessage(msg.key, orjson.dumps(flat_msg))

    @staticmethod
    def filter_message_type(msg, message_type):
        """Filter messages by the specified message type."""
        return orjson.loads(msg.value)['MessageType'].lower() == message_type.lower()
    
    @staticmethod
    def set_mmsi_key(msg):
        """Use MMSI key"""
        msg_json = orjson.loads(msg.value)

        return KafkaSinkMessage(msg_json['MMSI'], msg.value)
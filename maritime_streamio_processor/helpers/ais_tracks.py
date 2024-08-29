"""
AIS Tracks

This module provides a set of operations for parsing AIS (Automatic Identification System) messages in a Bytewax pipeline.

The operations interpret AIS message to determine the track type, surface or air. Surface tracks are vessels and SAR aircraft with altitude of zero or not reporting altitude. 

SAR aircraft with altitude greater than zero are air tracks.

"""
import orjson

class AISTrackParser:
    def __init__(self, msg_json):
        self.track_type = parse_track()

    def as_dict(self):
        return {
            'track_type': self.track_type,
        }
    

    def parse_track(self):
        return self.track_type

        
    

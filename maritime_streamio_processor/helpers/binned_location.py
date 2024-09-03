"""
AIS Bytewax Operations

This module provides a set of operations for processing AIS (Automatic Identification System) messages in a Bytewax pipeline.
The operations include formatting timestamps, enriching metadata, binning locations, calculating features, flattening JSON structures, and filtering messages by type.

The operations are designed to be used in a Bytewax pipeline to process AIS messages from a Kafka topic.
Each operation takes a `KafkaSinkMessage` object as input and returns a new `KafkaSinkMessage` object with the transformed data.

Dependencies:
  - `orjson`: a fast JSON parsing library
  - `flatten_json`: a library for flattening JSON structures
  - `bytewax`: a Python library for building data pipelines
  - `uuid`: a library for generating unique identifiers
  - `dateutil`: a library for parsing dates and times
"""
import h3
import s2sphere
import mgrs
import pygeohash as pgh

class BinnedLocation:
    def __init__(self, latitude, longitude, h3_resolution=8, s2_level=20, geohash_precision=8):
        self.lat_reported = latitude
        self.lng_reported = longitude        
        self.h3_resolution = h3_resolution
        self.s2_level = s2_level
        self.geohash_precision = geohash_precision
        
        # Bin location into the center of S2 Cell
        self.lat, self.lng = self.round_position()

        # Calculate the bins using updated lat and lng
        self.h3 = self.calculate_h3_bin()
        self.s2 = self.calculate_s2_bin()
        
        # Well Known Text format for geo compatibility
        # Note WKT lists longitude first
        self.wkt = f"POINT ({self.lng} {self.lat})"
        
        # MGRS Military Grid Reference System
        self.mgrs = self.calculate_mgrs()
        
        # geohashing
        # https://pypi.org/project/pygeohash/
        self.geohash = pgh.encode(self.lat,self.lng, precision=geohash_precision)


        
    '''Round location position to the center of a 100m circle using Google S2Sphere'''
    def round_position(self):
        s2_centering_level = 20 # ~95.46m
        s2_cell = s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(self.lat_reported, self.lng_reported)).parent(s2_centering_level)
        center = s2_cell.to_lat_lng()
        lat = round(center.lat().degrees,5)
        lng = round(center.lng().degrees,5)        
        return lat, lng
        
    '''Uber H3 spatial index using the center binned lat/lng'''
    def calculate_h3_bin(self):
        return h3.geo_to_h3(self.lat, self.lng, self.h3_resolution)
    
    '''Google S2 spatial index using the center binned lat/lng'''
    def calculate_s2_bin(self):
        lat_lng = s2sphere.LatLng.from_degrees(self.lat, self.lng)
        cell_id = s2sphere.CellId.from_lat_lng(lat_lng).parent(self.s2_level)
        return cell_id.id()
    
    '''MGRS Military Grid Reference System'''
    def calculate_mgrs(self):
        # MGRSPrecision=4 for 100m grid
        m = mgrs.MGRS()
        return m.toMGRS(self.lat, self.lng, MGRSPrecision=4)
    
    def as_dict(self):
        return {
            'h3': self.h3,
            's2': self.s2,
            'lat': self.latitude,
            'lng': self.longitude,
            'lat_reported': self.reported_lat,
            'lng_reported': self.reported_lng,
            'wkt': self.wkt,
            'mgrs': self.mgrs
        }
        
import numpy as np
import pandas as pd
import geoip2.database
import os


class IPGeoLocate(object):

    def __init__(self, con, latlong_db='GeoIP2-City.mmdb',
                 tables_columns={'*': 'ip_address'},
                 ipgeo_table='ipgeo'):
        self.con = con
        self.latlon_reader = geoip2.database.Reader(latlong_db)
        self.tables_columns = tables_columns
        self.ipgeo_table = ipgeo_table

    def _get_ips_to_locate(self, table, column, ipgeotable):
        query = 'SELECT DISTINCT {0} FROM {1} WHERE {0} NOT IN (SELECT ' \
                'ip_address FROM {2});'.format(column,
                                               table,
                                               ipgeotable)
        return (pd.read_sql(query, self.con))

    def update_ipgeo_table(self):
        # build a DF with all the IPs we need to geolocate
        ipdf = None
        for t, c in self.tables_columns.items():
            df = self._get_ips_to_locate(t, c, self.ipgeo_table)
            df.columns = ['ip_address']
            ipdf = df if not ipdf else pd.concat([ipdf, df])

        ipdf['ip_lon'] = [self.latlon_reader.city(ip).location.longitude
                          for ip in ipdf['ip_address']]
        ipdf['ip_lat'] = [self.latlon_reader.city(ip).location.latitude
                          for ip in ipdf['ip_address']]
        ipdf['ip_city'] = [self.latlon_reader.city(ip).city.name
                           for ip in ipdf['ip_address']]
        ipdf['ip_state'] = [self.latlon_reader.city(ip).subdivisions.
                            most_specific.iso_code for ip in
                            ipdf['ip_address']]
        ipdf['ip_zip_code'] = [self.latlon_reader.city(ip).postal.code
                               for ip in ipdf['ip_address']]
        ipdf['ip_country'] = [self.latlon_reader.city(ip).country.name
                              for ip in ipdf['ip_address']]
        ipdf['ip_country_iso'] = [self.latlon_reader.city(ip).country.iso_code
                                  for ip in ipdf['ip_address']]
        ipdf['admin_ne_id'] = np.nan

        self.con.load_table_columnar(self.ipgeo_table, ipdf)

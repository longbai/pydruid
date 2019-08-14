import os
import pytest
from pydruid.client import PyDruid
from pydruid.utils.aggregators import doublesum
from pydruid.utils.filters import Dimension

class TestCube:
    def test_cube_query(self):
    	query = PyDruid("http://pipeline.qiniu.com", 'v2/stream/cubes/query')
    	query.set_qiniu("", "")
    	top = query.topn(
    			datasource='domain_top_statics',
    			granularity='all',
    			intervals='2019-08-13/pt1h',  # utc time of 2014 oscars
    			aggregations={'count': doublesum('count')},
    			metric='count',
    			dimension='Country',
    			threshold=10)
    	df = query.export_pandas()
    	print(df)
    	top.export_tsv('top.tsv')
        

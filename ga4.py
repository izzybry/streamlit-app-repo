# Reference: https://learndataanalysis.org/source-code-automate-google-analytics-4-ga4-reporting-with-python-step-by-step-tutorial/

import os
import datetime
from typing import List, Tuple
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (Dimension, Metric, DateRange, Metric, OrderBy, 
                                               FilterExpression, MetricAggregation, CohortSpec)
from google.analytics.data_v1beta.types import RunReportRequest, RunRealtimeReportRequest

class GA4Exception(Exception):
    '''base class for GA4 exceptions'''
    
class GA4Report:
    def __init__(self, property_id, credentials):
        self.property_id = property_id
        self.client = BetaAnalyticsDataClient(credentials=credentials)

    def run_report(self, dimensions: List[str], metrics: List[Metric], date_ranges: List[Tuple[str, str]],
        offset_row: int=0, row_limit: int=10000, keep_empty_rows: bool=True, quota_usage: bool=False):
        """Returns a customized report of your Google Analytics event data.
        :param start_date: The inclusive start date for the query in the format YYYY-MM-DD.
        :param end_date: The inclusive end date for the query in the format YYYY-MM-DD.
        """
        try:
            dimension_list = [Dimension(name=dim) for dim in dimensions]
            metrics_list = [Metric(name=m) for m in metrics]
            # date_range = DateRange(start_date=start_date, end_date=end_date)
            date_ranges = [DateRange(start_date=date_range[0], end_date=date_range[1]) for date_range in date_ranges]

            report_request = RunReportRequest(
                property=f'properties/{self.property_id}',
                dimensions=dimension_list,
                metrics=metrics_list,
                limit=row_limit,
                return_property_quota=quota_usage,
                date_ranges=date_ranges,
                offset=offset_row,
                keep_empty_rows=keep_empty_rows
            )
            response = self.client.run_report(report_request)
     
            output = {}
            if 'property_quota' in response:
                output['quota'] = response.property_quota

            # construct the dataset
            headers = [header.name for header in response.dimension_headers] + [header.name for header in response.metric_headers]
            rows = []
            for row in response.rows:
                rows.append(
                    [dimension_value.value for dimension_value in row.dimension_values] + \
                    [metric_value.value for metric_value in row.metric_values])            

            output['headers'] = headers
            output['rows'] = rows
            output['row_count'] = response.row_count
            output['metadata'] = response.metadata
            output['response'] = response
            return output            
        except Exception as e:
            raise GA4Exception(e)
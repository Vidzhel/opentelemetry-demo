import os
from pathlib import Path

def get_metric_file(path):
    dir = '/'.join(path.split('/')[:-1])
    Path(dir).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(path):
        f = open(path, "w")
        f.write("time,value" + "\n")
        return f

    return open(path, "a")

def extract_attributes(attributes, target_attrs):
    results = {}
    for attribute in attributes:
        key = attribute['key']
        if key not in target_attrs:
            continue
        results[target_attrs[key]] = attribute['value']['stringValue']
    return results

def extract_sum_metric(metric, additional_info_extractor):
    results = []
    for dataPoint in metric['sum']['dataPoints']:
        result = {}
        result['time'] = dataPoint['timeUnixNano']

        if 'asInt' in dataPoint:
            result['value'] = int(dataPoint['asInt'])

        if 'asDouble' in dataPoint:
            result['value'] = float(dataPoint['asDouble'])

        results.append(additional_info_extractor(dataPoint, result))

    return results

def extract_histogram_metric(metric, additional_info_extractor):
    results = []
    for data_point in metric['histogram']['dataPoints']:
        result = {}
        result['value'] = data_point['sum'] / int(data_point['count'])
        result['time'] = data_point['timeUnixNano']
        results.append(additional_info_extractor(data_point, result))

    return results

def span_kind_to_type(kind):
    match kind:
        case 'SPAN_KIND_INTERNAL':
            return  'internal'
        case 'SPAN_KIND_CLIENT':
            return 'incoming'
        case 'SPAN_KIND_SERVER':
            return 'outgoing'
        case 'SPAN_KIND_CONSUMER':
            return 'incomingAsync'
        case 'SPAN_KIND_PRODUCER':
            return 'outgoingAsync'

class CallsMetric:
    @staticmethod
    def match(name):
        return name == 'calls'

    def __init__(self, metric):
        self.metrics = extract_sum_metric(metric, self.additional_info_extractor)

    def additional_info_extractor(self, data_point, result):
        metadata = extract_attributes(data_point['attributes'], {'span.kind': 'requestType', 'status.code': 'status'})
        result['type'] = span_kind_to_type(metadata['requestType'])
        if metadata['requestType'] == 'SPAN_KIND_INTERNAL':
            return result

        result['isError'] = metadata['status'].find("ERR") != -1
        return result

    def write(self, metadata):
        files_content = dict()
        for metric in self.metrics:
            error_part = 'without_errors'
            if metric.get('isError', False):
                error_part = 'with_errors'
            file_name = '{}_{}_requests_{}.csv'.format(metadata['serviceName'], metric['type'], error_part)
            if file_name not in files_content:
                files_content[file_name] = []
            files_content[file_name].append(metric)

        for file_name, metrics in files_content.items():
            f = get_metric_file("results/"+file_name)
            for metric in metrics:
                f.write('{},{}\n'.format(metric['time'], metric['value']))
            f.close()

class DurationMetric:
    @staticmethod
    def match(name):
        return name == 'duration'

    def __init__(self, metric):
        self.metrics = extract_histogram_metric(metric, self.additional_info_extractor)

    def additional_info_extractor(self, data_point, result):
        metadata = extract_attributes(data_point['attributes'], {'span.kind': 'requestType', 'status.code': 'status'})
        result['type'] = span_kind_to_type(metadata['requestType'])
        return result

    def write(self, metadata):
        files_content = dict()
        for metric in self.metrics:
            file_name = '{}_{}_request_duration.csv'.format(metadata['serviceName'], metric['type'])
            if file_name not in files_content:
                files_content[file_name] = []
            files_content[file_name].append(metric)

        for file_name, metrics in files_content.items():
            f = get_metric_file("results/"+file_name)
            for metric in metrics:
                f.write('{},{}\n'.format(metric['time'], metric['value']))
            f.close()

#!/usr/bin/python

#check_azure_sbus.py: Azure service bus monitor script

"""Contains the nagios azure storage plugin code."""

import argparse
import azure
import logging
import os
from azuremonitor.publishsettings import PublishSettings
from azure.servicemanagement import ServiceBusManagementService
import sys
from datetime import datetime
from datetime import timedelta
import exceptions

logger = None            # pylint: disable-msg=C0103

COUNTERS     = {
    'length'         : { 'help' : 'Get Length',
                            'measure'   :  'total',
                            'nagios_message' : 'Length %s',
                            'unit' : '',
                            #'direction' : 'NA'
                            },          
    'size'         : { 'help' : 'Get Max size',
                            'measure'   :  'total',
                            'nagios_message' : 'Max size %s',
                            'unit' : 'bytes',
                            #'direction' : 'NA'
                            },          
    'incoming'         : { 'help' : 'Get Incoming Messages',
                            'measure'   :  'total',
                            'nagios_message' : 'Total incoming messages %s',
                            'unit' : '',
                            #'direction' : 'NA'
                            },          
    'requests.total'         : { 'help' : 'Get Total Requests',
                            'measure'   :  'total',
                            'nagios_message' : 'Total number of requests %s',
                            'unit' : '',
                            #'direction' : 'NA'
                            },      
    'requests.successful': { 'help' : 'Get Total Succesful Requests',
                            'measure'   :  'total',
                            'nagios_message' : 
                                    'Total number of succesful requests %s',
                            'unit' : '',
                            #'direction' : 'NA'
                            },      
    'requests.failed': { 'help' : 'Get Total Failed Requests',
                            'measure'   :  'total',
                            'nagios_message' : 
                                    'Total number of failed requests %s',
                            'unit' : '',
                            #'direction' : 'NA'
                            },      
    'requests.failed.internalservererror': { 'help' : 'Get Total Internal Server Errors',
                            'measure'   :  'total',
                            'nagios_message' : 
                                    'Total number of failed internal server error requests %s',
                            'unit' : '',
                            #'direction' : 'NA'
                            },      
    'requests.failed.serverbusy': { 'help' : 'Get Total Server Busy Errors',
                            'measure'   :  'total',
                            'nagios_message' : 
                                    'Total number of failed server busy error requests %s',
                            'unit' : '',
                            #'direction' : 'NA'
                            },      
    'requests.failed.other': { 'help' : 'Get Total Other Errors',
                            'measure'   :  'total',
                            'nagios_message' : 
                                    'Total number of failed other error requests %s',
                            'unit' : '',
                            #'direction' : 'NA'
                            },      
    }


def property_value(row, prop):
    """Get the value of the row/object property specified by prop."""
    return {
       'total': row.total,
       'average': row.average,
       'min': row.min,
       'max': row.max
    }[prop]


def handle_args():
    """Create the parser, parse the args, and return them."""
    parser = argparse.ArgumentParser(description='Check Azure Service Bus',
                                     epilog='(c)')
    parser.add_argument('namespace', 
                        help='Service bus namespace name to check')
    parser.add_argument(
        '-p', '--publish-settings',
        required=True,
        help='.publishsettings file to authenticate with azure',
        dest='psfile')
    if os.name == 'nt':
        parser.add_argument(
            '-f', '--certname',
            required=False,
            help='cert authentication with azure. needed on Windows',
            dest='cert')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--topic', action='store_const', 
                       help="Check topic service",
                        const='topic', dest='type')
    group.add_argument('--notification', action='store_const',  
                       help="Check notification hub service",
                        const='notification', dest='type')
    group.add_argument('--queue', action='store_const',  
                       help="Check queue service",
                        const='queue', dest='type')
    group.add_argument('--relay', action='store_const',  
                       help="Check relay service",
                        const='relay', dest='type')

    parser.add_argument('-a', '--all', action='store_true',
                        help='Check all namespaces, ignores namespace name')

    parser.add_argument('-w', '--warning', required=False, dest='warning',
                        help='Specify warning threshold')
    parser.add_argument('-c', '--critical', required=False, dest='critical',
                        help='Specify critical threshold')
    parser.add_argument('-k', '--key', required=True, dest='key',
                        help='Status/Counter to check')
    parser.add_argument('-v', '--verbose', action='count', 
                        default=0, help='verbosity')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    return parser.parse_args()


def eval_counter_for_nagios(row, queue, counter, warning, critical, verbosity):
    """get the metric  for the key and check within the nagios range
    row - metric object
    queue - name of the queue/topic/etc
    counter - counter from COUNTERS dict
    warning -- Nagios warning range
    critical -- Nagios critical range
    """
    prop = counter['measure']    
    logger.debug("Eval: %s total:%s avg:%s min:%s max:%s" % (counter['help'], row.total, row.average, row.min, row.max))
    val = property_value(row, prop)
    unit = counter['unit']

    return nagios_eval(val, warning, critical, "%s for %s" % (counter['nagios_message'], queue), 
                       unit, verbosity)


def is_within_range(nagstring, value):
    """check if the value is withing the nagios range string
    nagstring -- nagios range string 
    value  -- value to compare
    Returns true if within the range, else false
    """
    if not nagstring:
        return False
    import re
    #import operator
    first_float = r'(?P<first>(-?[0-9]+(\.[0-9]+)?))'
    second_float = r'(?P<second>(-?[0-9]+(\.[0-9]+)?))'
    actions = [ (r'^%s$' % first_float, 
                    lambda y: (value > float(y.group('first'))) or (value < 0)),
                (r'^%s:$' % first_float, 
                    lambda y: value < float(y.group('first'))),
                (r'^~:%s$' % first_float, 
                    lambda y: value > float(y.group('first'))),
                (r'^%s:%s$' % (first_float,second_float), 
                    lambda y: (value < float(y.group('first'))) or 
                    (value > float(y.group('second')))),
                (r'^@%s:%s$' % (first_float,second_float), 
                    lambda y: not((value < float(y.group('first'))) or 
                                  (value > float(y.group('second')))))]
    for regstr, func in actions:
        res = re.match(regstr, nagstring)
        if res: 
            return func(res)
    raise Exception('Improper warning/critical parameter format.')


def nagios_eval(result, warning, critical, nagios_message, unit='', 
                verbosity = 0):
    """evaluate result with respect to warning and critical range and 
        return appropriate error message
    result -- counter value
    warning -- nagios warning range string
    critical -- nagios critical range string
    nagios_message -- Nagios message 
    unit -- unit for the perf counter value
    verbosity -- nagios verbosity value
    Returns nagios code, and error message
    """

    if is_within_range(critical, result):
        prefix = 'CRITICAL:'
        code = 2
    elif is_within_range(warning, result):
        prefix = 'WARNING:'
        code = 1
    else:
        prefix = 'OK:'
        code = 0
    strresult = str(result)
    if verbosity == 0:
        if code > 0:
            nagios_message = '%s' % prefix
        else:
            nagios_message = ''
    elif verbosity == 1:
        if code > 0:
            nagios_message = nagios_message % (strresult)
            nagios_message = '%s:%s=%s %s' % ( prefix, nagios_message, 
                                              strresult,  unit or '')
        else:
            nagios_message = ''
    else:
        nagios_message = nagios_message % (strresult)
        nagios_message = '%s%s%s,warning=%s,critical=%s,'\
         % ( prefix, nagios_message, unit or '', warning or '', critical or '')
    return code, nagios_message


def setup_logger(verbose):
    """Creates a logger, using the verbosity, and returns it."""
    global logger
    logger = logging.getLogger()
    if verbose >= 3:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)        
    logger.addHandler(logging.StreamHandler())


def check_sbus_errors_all(management, sbus_type, key, warning, 
                             critical, verbosity):
    """Check storage errors for the metric given by key
    management -- service bus management object
    type -- topic/queue/relay/notification (hub)
    key - needed 
    warning - Nagios warning level 
    critical - Nagios critical level 
    """
    error_code_all = 0
    errors = []
    namespaces = management.list_namespaces()

    for namespace in namespaces:
        error_code, error = check_sbus_errors(management, 
                                                 namespace.name,
                                                 None,
                                                 sbus_type, 
                                                 key, 
                                                 warning, 
                                                 critical, 
                                                 verbosity)
        error_code_all = max(error_code_all, error_code)
        errors.append(namespace.name + ':{'+error + '}')
    return error_code_all, ', '.join(errors)


def check_sbus_errors(management, namespace, qname, sbus_type, key,
                         warning, critical, verbosity):
    """Check service bus errors for the metric given by key
    management -- service management object
    namespace -- service bus namespace name
    type -- topic/queue/relay/notification (hub)
    key - needed 
    warning - Nagios warning level 
    critical - Nagios critical level 
    """
    
    errors = []
    try:
        latest_utcime = datetime.utcnow()
        latest_hour = (latest_utcime-timedelta(hours=2)).strftime('%Y-%m-%dT%H:00:00Z')
        recenthour_filter = '$filter=Timestamp ge datetime\'%s\'' % latest_hour
        rollup = "PT5M" # PT5M PT1H P1D P7D

        sbus_type = sbus_type.lower()

        rows = {}
        metrics = []
        if sbus_type == 'topic':
            for queue in management.list_topics(namespace):
                if qname and queue.name != qname:
                        continue
                metrics = management.get_supported_metrics_topic(namespace, queue.name)
                if key != 'all':
                    metrics = [metric for metric in metrics if metric.name == key]
                for metric in metrics:
                    if not metric.name in rows:
                        rows[metric.name] = {}
                    rows[metric.name][queue.name] = management.get_metrics_data_topic(namespace, queue.name, metric.name, rollup, recenthour_filter)
        elif sbus_type == 'queue':
            for queue in management.list_queues(namespace):
                if qname and queue.name != qname:
                        continue
                metrics = management.get_supported_metrics_queue(namespace, queue.name)
                if key != 'all':
                    metrics = [metric for metric in metrics if metric.name == key]
                for metric in metrics:
                    if not metric.name in rows:
                        rows[metric.name] = {}
                    rows[metric.name][queue.name] = management.get_metrics_data_queue(namespace, queue.name, metric.name, rollup, recenthour_filter)
        elif sbus_type == 'relay':
            print "TODO not implemented"
            sys.exit(3)
        elif sbus_type == 'notification':
            print "TODO not implemented"
            sys.exit(3)
        else:
            print "invalid operation %s" % sbus_type
            sys.exit(3)

#        for metric in rows:
#            for queue in rows[metric]:
#                logger.debug("collected data for %s/%s" % (queue,metric))
#                for row in rows[metric][queue]:
#                    logger.debug("%s total:%s min:%s max:%s avg:%s" % (row.timestamp, row.total, row.min, row.max, row.average))
       
        current_counters = {}
        
        if key == 'all':
            # for inspecting all keys, we can't use critical or warning levels
            current_counters = COUNTERS
            warning = None
            critical = None
        else:
            current_counters[key] = COUNTERS[key]

        error_code_all = 0
        errors = []
        for metric in current_counters:
            counter = COUNTERS[metric]

            for queue in rows[metric]:
                temp_rows = rows[metric][queue]
                #logger.debug("%s/%s" % (metric, queue))
                if len(temp_rows) > 0:
                    row = temp_rows[len(temp_rows)-1]

                    error_code, error = eval_counter_for_nagios(row, queue, counter, warning, 
                                                                critical, verbosity )
                    error_code_all = max(error_code_all, error_code)
                    errors.append(error)        
                else:
                    error_code = 0
                    #errors.append("OK: No data %s/%s" % (queue,metric))
                    logger.debug('Metrics data not available for %s/%s' % (queue,metric))

    except azure.WindowsAzureMissingResourceError, error:
        error_code_all = 3
        errors.append('Metrics data not found.')
    except exceptions.KeyError, error:
        error_code_all = 3
        errors.append('Specified key was not found.')
    return error_code_all, '; '.join(errors)



def main():
    """Main procedure for Azure monitor utility."""
    global logger

    error = ''
    error_code = 0
    args = handle_args()

    if not args.all and not args.namespace:
        print 'Service bus namespace name missing'        
        sys.exit(3)

    setup_logger(args.verbose)
    logger.debug('Converting publishsettings.')
    try:
        publishsettings = PublishSettings(args.psfile)
    except error:
        print 'Publishsettings file is not good. Error %s' % error
        sys.exit(1)
    if os.name != 'nt':
        pem_path = publishsettings.write_pem()
        logger.debug('Pem file saved to temp file {0}'.format(pem_path))
        logger.debug('Azure sub id {0}'.format(publishsettings.sub_id))
        management = ServiceBusManagementService(
            subscription_id=publishsettings.sub_id,
            cert_file=pem_path)
    else:
        logger.debug('Using cert to instantiate ServiceBusManagement.')
        if args.cert:
            management = ServiceBusManagementService(
                publishsettings.sub_id,
                cert_file=args.cert)
        else:
            print 'Cert is missing. Required on Windows'
            sys.exit(3)

    if not args.key:
        print 'Key is required.'
        sys.exit(3)
    if args.all:
        error_code, error = check_sbus_errors_all(management,  
                                                     args.type, 
                                                     args.key, 
                                                     args.warning, 
                                                     args.critical, 
                                                     args.verbose)
    else:
        namespace = None
        queue = None
        temp = args.namespace.lower().split(':', 1)
        if (len(temp) > 1):
            namespace, queue = temp
        else:
            namespace = temp

        error_code, error = check_sbus_errors(management, 
                                                 namespace, 
                                                 queue,
                                                 args.type,
                                                 args.key, 
                                                 args.warning, 
                                                 args.critical, 
                                                 args.verbose)

    if os.name != 'nt':
        os.unlink(pem_path)
        logger.debug('Deleted pem.')

    if error_code == 0 and not error:
        error = 'All OK'
    print error
    sys.exit(error_code)


if __name__ == '__main__':
    main()

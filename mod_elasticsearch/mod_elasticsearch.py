#!/usr/bin/env python
import socket
import sys
import time
import re
import json
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from datetime import datetime,timedelta
import time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""This Module does query to elasticsearch . Written on top of python-elasticsearch module"""

#DEFAULT CONFIG FILE
CONFIG_FILE='/etc/config/kibanaConfig.json'


class ElasticQuery(object):
        """Class for elasticsearch query \n Optional args: index,configfile"""
        
        def __init__(self,index="logstash-*",file=CONFIG_FILE):
            """ Initialization function for reading config and creating connection object with elasticsearch """
            
            self.index=index
            self.configfile=file

            try:
                with open(self.configfile, 'r') as config_file:
                    self.config_data = json.load(config_file)
                self.es = Elasticsearch([self.config_data['elastic']['host']], port=self.config_data['elastic']['port'], connection_class=RequestsHttpConnection, http_auth=(self.config_data['elastic']['user'], self.config_data['elastic']['password']), use_ssl=True, verify_certs=False,retry_on_timeout=True)        
                if self.es.ping() is False:
                    raise Exception("Host is not pingable")     

                #increase the window size of result being returned. BY defaulut:10    
            except IOError:
                print("Config file not found at {0}".format(self.configfile))
                sys.exit(1)
            except KeyError:
                print("Error in decoding json config file. {0}".format(self.configfile))
                sys.exit(1)
            except Exception as unknown:
                print(unknown)
                sys.exit(1)

            
        def __epochTime(self,*args):
            """GETTING EPOCH TIME FOR QUERY DATES"""
            try:
                if len(args) ==2:
                    starttime= datetime.strptime(args[0],'%Y-%m-%d %H:%M')
                    endtime = datetime.strptime(args[1],'%Y-%m-%d %H:%M')
                    if( starttime > endtime ):
                        print("starting time can't be greater than end time")
                        sys.exit(1)
                elif len(args) ==1:
                    endtime = datetime.now()
                    day = re.match('^(\d+)d$',args[0])
                    hour = re.match('^(\d+)h$',args[0])
                    minute = re.match('^(\d+)m$',args[0])
                    
                    if day:
                        starttime = endtime - timedelta(days=int(day.group(1)))
                    elif hour:
                        starttime = endtime - timedelta(hours=int(hour.group(1)))
                    elif minute:   
                        starttime = endtime - timedelta(minutes=int(minute.group(1)))
                    else:
                        print("Unsupported Time format")
                        sys.exit(1)
                else:
                    print("Requried Min args 1 or a max args 2")
                    sys.exit()
                return [self.__getMilliSeconds(starttime) ,self.__getMilliSeconds(endtime)]
            except Exception as e:
                print("Error Occured {0}".format(e))
                sys.exit(1)

        
        def  __getMilliSeconds(self,date):
            """GET EPOCH MILLISEC FROM DATE"""
            return time.mktime(date.timetuple()) *1000
              

        
        def __composeBody(self,query,*args):
            """ COMPOSE REQUEST BODY """
            [starttime,endtime] = self.__epochTime(*args)
            self.body ={ 
                     "query": { 
                            "bool": { 
                                "must": [ {
                                        "query_string" : { 
                                             "query": query,
                                             "analyze_wildcard": "true" 
                                             }
                                        }],
                                        "filter": [{ 
                                            "range": {
                                                 "@timestamp": {
                                                     "gte": starttime, 
                                                     "lte": endtime, 
                                                     "format": "epoch_millis"
                                                      }
                                                    }
                                                }] 
                                    }
                                }
                }
            try:    
                self.body = json.dumps(self.body)
            except Exception as e:
                 print("Error Occured {0}".format(e))
                 sys.exit(1)

        def query(self,query,timeinterval,count=False,*args):
                """QUERY FUNCTIONS AND RETURN RESULTS \n Required arguments: query,timeinterval  \n Optional arguments: endtime,count \n supported time format: YYYY-MM-DD HH:MM,[0-9+][m|h|d]\n EX :-obectname.query('host:eas* AND InvalidSyncKey',timeinterval='15m' \n obectname.query('host:eas* AND InvalidSyncKey',timeinterval=['starttime','endtime'],count=True """
                try:
                    if type(timeinterval) is str:
                        self.__composeBody(query,timeinterval)
                       
                    elif type(timeinterval) is list:
                        if len(timeinterval) == 2:
                            self.__composeBody(query,timeinterval[0],timeinterval[1])
                        else:
                            print("Wrong time interval provided. EX:- timeinterval=['starttime','endtime']")
                            sys.exit(1)
                    else:
                         print("Wrong time interval provided. EX:- timeinterval='2h',timeinterval=['starttime','endtime']")
                         sys.exit(1)

                    # SUPPORT FILTERING FIELDS AND HIT COUNT RECORD
                    if count == False :
                        page= self.es.search(index=self.index,scroll = '2m',size=10, body=self.body,request_timeout=self.config_data['elastic']['timeout'])
                        sid = page['_scroll_id']
                        scroll_size = page['hits']['total']

                        #check record return before processing too avoid getting killed due to oom.
                        if(scroll_size > 2000000):
                            print("Result returned from query is {0}. Limit is 20 Lakh records.please consider changing the time interval.".format(scroll_size))
                            sys.exit()
                        
                        #Process the current batch
                        data=[]
                        data.append(page['hits']['hits'])
                        # Start scrolling
                        while (scroll_size > 0):
                            page = self.es.scroll(scroll_id = sid, scroll = '2m')
                            # Update the scroll ID
                            sid = page['_scroll_id']
                            # Get the number of results that we returned in the last scroll
                            scroll_size = len(page['hits']['hits'])
                            data.append(page['hits']['hits'])
                        return data
                    else:
                       return self.es.count(index=self.index, body=self.body, request_timeout=self.config_data['elastic']['timeout'])

                except Exception as e :
                    print ("Error Occured {0}".format(e))
                    sys.exit

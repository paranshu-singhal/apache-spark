import sys
import time
from urllib import urlencode
from json import loads, dumps
from urllib2 import URLError, urlopen

from pyspark.sql import SparkSession


def spark(sourceFile, startTime, endTime):
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Programming Assignment #4") \
        .getOrCreate()

    df = spark.read.option("sep", "\t").csv(sourceFile)

    time1 = time.time()
    rows = df.select(df['_c5'],df['_c6']).collect()
    time2 = time.time()

    print("SparkSQL: Time to query for taskA: "+ str((time2 - time1)*1000) + " milliseconds.")

    hashset = {}
    hashsetcount = {}

    for row in rows:
        if row['_c5'] not in hashset:
            hashset[row['_c5']] = int(row['_c6'])
            hashsetcount[row['_c5']] = 1
        else:
            hashset[row['_c5']] += int(row['_c6'])
            hashsetcount[row['_c5']] += 1

    result = ""
    for code in hashset:
        hashset[code] = round((hashset[code] / hashsetcount[code]), 6)
        result += 'Code ' + str(code) + ', average number of bytes = ' + str(hashset[code]) + "\n"
    f = open("taskA.txt", "w")
    f.write(result)
    f.close()

    time1 = time.time()
    count = df.filter(df['_c2'] >= startTime).filter(df['_c2'] <= endTime).count()
    time2 = time.time()
    print("SparkSQL: Time to query for taskB: "+ str((time2 - time1)*1000) + " milliseconds.")

    result = ("No of entries: "+ str(count) +"\n")
    f = open("taskB.txt", "w")
    f.write(result)
    f = open("taskC.txt", "w")
    f.write(result)
    f.close()

def asterixdb(sourceFile, startTime, endTime):
    def build_response(data, endpoint = "query/service"):
        api_endpoint = "http://localhost:19002/" + endpoint

        try:
            urlresponse = urlopen(api_endpoint, data=urlencode(data))

            urlresult = ""
            CHUNK = 16 * 1024
            while True:
                chunk = urlresponse.read(CHUNK)
                if not chunk: break
                urlresult += chunk
            return loads(urlresult)

        except URLError as e:

            # Here we report possible errors in request fulfillment.
            if hasattr(e, 'reason'):
                print ('Failed to reach a server.')
                print ('Reason: ', e.reason)

            elif hasattr(e, 'code'):
                print ('The server couldn\'t fulfill the request.')
                print ('Error code: ', e.code)

    def createDataset():
        data = {'statement': 'drop dataset NasaDataset if exists; drop type NasaType if exists; create type NasaType as closed { host: string, logname: string, time: int32, method: string, URL: string, response: string, bytes: int32, referrer: string, useragent: string }; create external dataset NasaDataset(NasaType) using hdfs (("hdfs"="hdfs://localhost:9000"), ("path"="'+sourceFile+'"), ("input-format"="text-input-format"), ("format"="delimited-text"), ("delimiter"="\t")); '}
        print(data)
        build_response(data)
    
    def taskA():
        data = { 'statement': 'SELECT DISTINCT response from NasaDataset;'}
        response = build_response(data)
        print('AsterixDB: elapsedTime for taskA: '+response['metrics']['elapsedTime'])

        result = ""
        for r in response['results']:
            code = r['response']
            data = { 'statement': 'ARRAY_AVG ( (SELECT VALUE bytes FROM NasaDataset WHERE response = "'+ code +'"));' }
            response = build_response(data)
            result += ("Code " + code +", average number of bytes = " + str(response['results'][0])) + "\n"
        
    def taskB():
        data = {'statement': 'DROP INDEX NasaDataset.NasaIdx IF EXISTS;'}
        build_response(data)

        data = { 'statement': 'ARRAY_COUNT ( (SELECT * FROM NasaDataset ns WHERE ns.time >=' + startTime + ' AND ns.time <= '+ endTime +'));' }
        response = build_response(data)
        print('AsterixDB: elapsedTime for taskB: '+response['metrics']['elapsedTime'])
        result = ("No of entries: "+ str(response['results'][0]) +"\n")


    def taskC():
        data = {'statement': 'CREATE INDEX NasaIdx on NasaDataset(time);'}
        build_response(data)

        data = { 'statement': 'ARRAY_COUNT ( (SELECT * FROM NasaDataset ns WHERE ns.time >=' + startTime + ' AND ns.time <= '+ endTime +'));' }
        response = build_response(data)
        print('AsterixDB: elapsedTime for taskC: '+response['metrics']['elapsedTime'])
        result = ("No of entries: "+ str(response['results'][0]) +"\n")
    
    #createDataset(args[1])
    taskA()
    taskB()
    taskC()    


args = sys.argv
sourceFile = "hdfs://localhost:9000" + args[1]
startTime = args[2]
endTime = args[3]

spark(sourceFile, startTime, endTime)
asterixdb(sourceFile, startTime, endTime)
from datetime import datetime
from elasticsearch import Elasticsearch
from numpy import *
import numpy
import time

S_ES_LIMIT = '10000'
S_GRANULARITY = '100'

#Classe de Tweet
class cTweet:
    def __init__(self, lTweet):
        self.sText = lTweet['text']
        self.sId = lTweet['id_str']
        self.sTimestamp = lTweet['timestamp_ms'][:-3]

    def getText(self):
        return self.sText

    def getId(self):
        return self.sId

    def getTimestamp(self):
        return self.sTimestamp

    def getTweet(self):
        return [self.sId, self.sTimestamp, self.sText]


#Classe Iterateur de cTweetPack
class cPackIterator:
    def __init__(self, tweetPack):
        self.index = 0
        self.tweetPack = tweetPack

    def hasNext(self):
        if self.index + 1 >= len(self.tweetPack):
            bResponse = False
        else:
            bResponse = True
        return bResponse

    def next(self):
        tweet = self.tweetPack[self.index]
        self.index += 1

    def getTweet(self):
        return self.tweetPack[self.index]


#Classe iterable contenant les packs de tweets
class cTweetPack:
    def __init__(self):
        self.tweet = []

    def add(self, lTweet):
        self.tweet.append(lTweet)

    def iterator(self):
        return cPackIterator(self.tweet)


#num
class cBatchNumber:
    def __init__(self, xEs, sIndexName, sDocTypeName, sBatchSize, sTimestampInit, sTimestampEnd):
        self.xEs = xEs
        self.sIndexName = sIndexName
        self.sDocTypeName= sDocTypeName
        self.sTimestampInit = sTimestampInit
        self.sBatchSize = sBatchSize
        self.xTweetPack = cTweetPack()
        self.nCurrentSize = 1
        self.nIndexSize = int(self.xEs.count(index = self.sIndexName)['count'])

        xResponse = self.xEs.search(index = self.sIndexName, doc_type = self.sDocTypeName, size ='1',
                                    sort = ['timestamp_ms:asc'], _source = 'timestamp_ms', stored_fields = 'timestamp_ms',
                                    body = {'query': {'match_all': {}}})
        for hit in xResponse['hits']['hits']:
            self.sDateInit = hit['_source']['timestamp_ms']

        if int(self.sTimestampInit) > int(self.sDateInit[:-3]):
            self.sTimestampInit = str(int(self.sTimestampInit) * 1000)
            self.sDateBegin = self.sTimestampInit
        else:
            self.sDateBegin = self.sDateInit

        lFields = ['text', 'id_str', 'timestamp_ms']
        if sBatchSize < S_ES_LIMIT :
            xResponse = self.xEs.search(index = self.sIndexName, doc_type = self.sDocTypeName, scroll ='10m', size = sBatchSize,
                                        sort = ['timestamp_ms:asc'], _source = lFields, stored_fields = lFields,
                                        body = {'query': {'range': {'timestamp_ms': {'gte': self.sTimestampInit}}}})
            for hit in xResponse['hits']['hits']:
                self.xTweetPack.add(hit['_source'])
                self.sLastTimestamp = hit['_source']['timestamp_ms']
                self.sLastTimestamp = str(int(self.sLastTimestamp) + 1)
                self.nCurrentSize += 1
        else :

            nTmpCalc = int(sBatchSize) - int(S_GRANULARITY)
            sBatchSize = str(nTmpCalc)
            xResponse = self.xEs.search(index = self.sIndexName, doc_type = self.sDocTypeName, scroll ='10m', size = S_GRANULARITY,
                                        sort = ['timestamp_ms:asc'], _source = lFields, stored_fields = lFields,
                                        body = {'query': {'range': {'timestamp_ms': {'gte': self.sTimestampInit}}}})
            self.sScroll = xResponse['_scroll_id']
            for hit in xResponse['hits']['hits']:
                self.xTweetPack.add(hit['_source'])
                self.sLastTimestamp = hit['_source']['timestamp_ms']
                self.sLastTimestamp = str(int(self.sLastTimestamp) + 1)
            while (int(sBatchSize) > 0):
                try:
                    rs2 = self.xEs.scroll(scroll_id = self.sScroll, scroll ='10m')
                    self.sScroll = rs2['_scroll_id']
                    nTmpCalc = int(sBatchSize) - int(S_GRANULARITY)
                    sBatchSize = str(nTmpCalc)
                    for hit in rs2['hits']['hits']:
                        self.xTweetPack.add(hit['_source'])
                        self.sLastTimestamp = hit['_source']['timestamp_ms']
                        self.sLastTimestamp = str(int(self.sLastTimestamp) + 1)
                        self.nCurrentSize += 1
                except:
                    break


    def hasNext(self):
        if self.nCurrentSize + 1 < self.nIndexSize:
            bResponse = True
        else:
            bResponse = False
        return bResponse

    def next(self):
        lFields = ['text', 'id_str', 'timestamp_ms']
        if self.nIndexSize - self.nCurrentSize < int(self.sBatchSize):
            nTmpSize = self.nIndexSize - int(self.sBatchSize)
        else:
            nTmpSize = int(self.sBatchSize)
        self.xTweetPack = cTweetPack()
        if nTmpSize < int(S_ES_LIMIT) :
            xResponse = self.xEs.search(index = self.sIndexName, doc_type = self.sDocTypeName, scroll ='10m', size = nTmpSize,
                                        sort = ['timestamp_ms:asc'], _source = lFields, stored_fields = lFields,
                                        body={'query': {'range': {'timestamp_ms': {'gte': self.sLastTimestamp}}}})
            self.sScroll = xResponse['_scroll_id']
            for hit in xResponse['hits']['hits']:
                self.xTweetPack.add(hit['_source'])
                self.sLastTimestamp = hit['_source']['timestamp_ms']
                self.sLastTimestamp = str(int(self.sLastTimestamp) + 1)
                self.nCurrentSize += 1
        else :
            nTmpSize -= int(S_GRANULARITY)
            xResponse = self.xEs.search(index = self.sIndexName, doc_type = self.sDocTypeName, scroll ='10m', size = S_GRANULARITY,
                                        sort = ['timestamp_ms:asc'], _source = lFields, stored_fields = lFields,
                                        body = {'query': {'range': {'timestamp_ms': {'gte': self.sLastTimestamp}}}})
            self.sScroll = xResponse['_scroll_id']
            for hit in xResponse['hits']['hits']:
                self.xTweetPack.add(hit['_source'])
                self.sLastTimestamp = hit['_source']['timestamp_ms']
                self.sLastTimestamp = str(int(self.sLastTimestamp) + 1)
                self.nCurrentSize += 1
            while (nTmpSize > 0):
                try:
                    rs2 = self.xEs.scroll(scroll_id = self.sScroll, scroll ='10m')
                    self.sScroll = rs2['_scroll_id']
                    nTmpSize -= int(S_GRANULARITY)
                    for hit in rs2['hits']['hits']:
                        self.xTweetPack.add(hit['_source'])
                        self.sLastTimestamp = hit['_source']['timestamp_ms']
                        self.sLastTimestamp = str(int(self.sLastTimestamp) + 1)
                        self.nCurrentSize += 1
                except:
                    break

    def getPack(self):
        return self.xTweetPack

    def getIndexSize(self):
        return self.nIndexSize

    def getCurrentLine(self):
        return self.nCurrentSize


class cBatchTime:
    def __init__(self, xEs, sIndexName, sDocTypeName, sTimestampInit, sBatchSizeHour):
        self.xEs = xEs
        self.nCurrentSize = 1
        self.sIndexName = sIndexName
        self.sDocTypeName = sDocTypeName
        self.sTimeStampInit = sTimestampInit
        self.sBatchSizeHour = sBatchSizeHour
        self.xTweetPack = cTweetPack()
        self.nIndexSize = int(self.xEs.count(index = self.sIndexName)['count'])


        lFields = ['text', 'id_str', 'timestamp_ms']
        xResponse = self.xEs.search(index = self.sIndexName, doc_type = self.sDocTypeName, size ='1',
                                    sort = ['timestamp_ms:asc'], _source = 'timestamp_ms', stored_fields = 'timestamp_ms',
                                    body = {'query': {'match_all': {}}})
        for hit in xResponse['hits']['hits']:
            self.sDateInit = hit['_source']['timestamp_ms']

        if int(self.sTimeStampInit) > int(self.sDateInit[:-3]):
            self.sTimeStampInit = str(int(self.sTimeStampInit) * 1000)
            self.sTimeBegin = self.sTimeStampInit
        else:
            self.sTimeBegin = self.sDateInit

        nTmpTime = int(self.sTimeBegin) + (int(self.sBatchSizeHour) * 60000 * 60)
        self.sTimeEnd = str(nTmpTime)

        xResponse = self.xEs.search(index = self.sIndexName, doc_type = self.sDocTypeName, size = S_ES_LIMIT, scroll ='10s',
                                    sort = ['timestamp_ms:asc'], _source = lFields, stored_fields = lFields,
                                    body = {'query': {'range': {'timestamp_ms': {'gte': self.sTimeBegin, 'lte': self.sTimeEnd}}}})

        for hit in xResponse['hits']['hits']:
            self.xTweetPack.add(hit['_source'])
            self.sLastTimestamp = self.sTimeEnd
            self.sLastTimestamp = str(int(self.sLastTimestamp) + 1)
            self.nCurrentSize += 1

        while (1):
            try:
                rs2 = self.xEs.scroll(scroll_id = self.sScroll, scroll ='10s')
                self.sScroll = rs2['_scroll_id']
                for hit in rs2['hits']['hits']:
                    self.xTweetPack.add(hit['_source'])
                    self.nCurrentSize += 1
            except:
                break

    def hasNext(self):
        if self.nCurrentSize + 1 < self.nIndexSize:
            bResponse = True
        else:
            bResponse = False
        return bResponse

    def next(self):
        self.xTweetPack = cTweetPack()
        lFields = ['text', 'id_str', 'timestamp_ms']
        self.nCurrentSize = 0

        self.sTimeBegin = self.sLastTimestamp
        tmpInt = int(self.sTimeBegin) + (int(self.sBatchSizeHour) * 60000 * 60)
        self.sTimeEnd = str(tmpInt)

        rs = self.xEs.search(index = self.sIndexName, doc_type = self.sDocTypeName, size = S_ES_LIMIT, scroll ='10m',
                             sort=['timestamp_ms:asc'], _source = lFields, stored_fields = lFields,
                             body={'query': {'range': {'timestamp_ms': {'gte': self.sTimeBegin, 'lte': self.sTimeEnd}}}})

        for hit in rs['hits']['hits']:
            self.xTweetPack.add(hit['_source'])
            self.sLastTimestamp = self.sTimeEnd
            self.sLastTimestamp = str(int(self.sLastTimestamp) + 1)
            self.nCurrentSize += 1

        while (1):
            try:
                rs2 = self.xEs.scroll(scroll_id = self.sScroll, scroll ='10s')
                self.sScroll = rs2['_scroll_id']
                for hit in rs2['hits']['hits']:
                    self.xTweetPack.add(hit['_source'])
                    self.nCurrentSize += 1
            except:
                break

    def getPack(self):
        return self.xTweetPack

    def getIndexSize(self):
        return self.nIndexSize

    def getCurrentLine(self):
        return self.nCurrentSize

#Classe de connection a ES
class cHandleEs:
    def __init__(self):
        self.sCluster = ''

    def __init__(self, sName):
        self.sCluster = sName

    def connectionToEs(self):
        if len(self.sCluster):
            es = Elasticsearch(cluster=self.sCluster)
        else:
            es = Elasticsearch()
        return es


def trimPunctuation(sWordInit):
    while len(sWordInit) > 0 and (sWordInit[0] == '.' or sWordInit[0] == ','):
        sWordInit = sWordInit[1:]
    while len(sWordInit) > 0 and (sWordInit.endswith('.') or sWordInit.endswith(',')):
        sWordInit = sWordInit[1:]
    return sWordInit

def test():
    tSeed = ['#Lyon', '#lyon', '#lumignons', '#FDL2016', '#OnlyLyon', '#Onlyon']
    #tSeed = ['Lyon',  '#Lyon', 'FDL', '#FDL', 'lumignons', '#FDL2016', 'FDL2016', 'OnlyLyon', 'Onlyon']
    i = 0
    n = 1
    t = 1
    p = 0
    dUnique = {}
    nNbBatch = 1


    handleEs = cHandleEs('test_app')

    es = handleEs.connectionToEs()
   # bt = cBatchTime(es, 'twitter2015', 'tweet', '')

    bt = cBatchTime(es, 'twitter2015', 'tweet', '1481115601', '1')

    while bt.hasNext():
        it = bt.getPack().iterator()

        while it.hasNext():
            sCurrent = it.getTweet()['text']
            it.next()
            tCurrentSplit = sCurrent.split()
            t += 1
            for each in tCurrentSplit:
                #if each[0] == '#':

                each = trimPunctuation(each)

                if dUnique.get(each) == None:
                    dUnique[each] = i
                    i += 1
        mCooc = numpy.zeros((len(dUnique), len(dUnique)))
        it = bt.getPack().iterator()
        i = 0

        while it.hasNext():
            sCurrent = it.getTweet()['text']
            it.next()
            tCurrentSplit = sCurrent.split()
            n = 0
            for each in tCurrentSplit:

                each = trimPunctuation(each)

                #if each[0] == '#':
                p = n + 1
                while p < len(tCurrentSplit):
                    #if tCurrentSplit[p][0] == '#':
                    a = dUnique.get(each)
                    b = dUnique.get(tCurrentSplit[p])
                    mCooc[a, b] += 1
                    mCooc[b, a] += 1
                    p += 1
                n += 1
        mResult = numpy.zeros(len(dUnique))

        for sSeed in tSeed:
            a = dUnique.get(sSeed)
            if a != None:
                p = 0
                max1 = p
                max2 = p
                max3 = p
                max4 = p
                max5 = p
                while p < len(dUnique):
                    w = mCooc[a, p]
                    x = mCooc[a, max1]
                    y = mCooc[a, max2]
                    z = mCooc[a, max3]
                    w_ = mCooc[a, max4]
                    x_ = mCooc[a, max5]
                    if w > x:
                        max5 = max4
                        max4 = max3
                        max3 = max2
                        max2 = max1
                        max1 = p
                    elif w > y:
                        max5 = max4
                        max4 = max3
                        max3 = max2
                        max2 = p
                    elif w > z:
                        max5 = max4
                        max4 = max3
                        max3 = p
                    elif w > w_:
                        max5 = max4
                        max4 = p
                    elif w > x_:
                        max5 = p
                    p += 1


                """
                sValueMax = [c for c,v in dUnique.items() if v==max1]
                print(sSeed + ' coocure le plus avec -> ' + sValueMax[0])
                sValueMax = [c for c,v in dUnique.items() if v==max2]
                print(sSeed + ' coocure puis avec -> ' + sValueMax[0])
                sValueMax = [c for c,v in dUnique.items() if v==max3]
                print(sSeed + ' coocure puis avec -> ' + sValueMax[0])"""


            if a != None:
                p = 0
                while p < len(dUnique):
                    mResult[p] += mCooc[a, p]
                    p += 1



        for sSeed in tSeed:
            a = dUnique.get(sSeed)
            if a != None:
                mResult[a] = -1




        p = 0
        max1 = 0
        max2 = 0
        max3 = 0
        max4 = 0
        max5 = 0
        while p < len(dUnique):
            w = mResult[p]
            x = mResult[max1]
            y = mResult[max2]
            z = mResult[max3]
            w_ = mResult[max4]
            x_ = mResult[max5]
            if w > x:
                max5 = max4
                max4 = max3
                max3 = max2
                max2 = max1
                max1 = p
            elif w > y:
                max5 = max4
                max4 = max3
                max3 = max2
                max2 = p
            elif w > z:
                max5 = max4
                max4 = max3
                max3 = p
            elif w > w_:
                max5 = max4
                max4 = p
            elif w > x_:
                max5 = p
            p += 1



        """print('--- ' + str(nNbBatch) + ' ---')
        print('1 -> ' + str(max1) + ' ||| 2 -> ' + str(max2) + ' ||| 3 -> ' + str(max3))
        sValueMax = [c for c, v in dUnique.items() if v == max1]
        if len(sValueMax) > 0 and max1 != 0:
            print('fist -> ' + sValueMax[0])
        sValueMax = [c for c,v in dUnique.items() if v==max2]
        if len(sValueMax) > 0 and max1 != 0:
            print('then -> ' + sValueMax[0])
        sValueMax = [c for c,v in dUnique.items() if v==max3]
        if len(sValueMax) > 0 and max1 != 0:
            print('then -> ' + sValueMax[0])"""



        sValueMax = [c for c, v in dUnique.items() if v == max1]
        if len(sValueMax) > 0 and max1 != 0:
            mot1 = sValueMax[0]
        else:
            mot1 = ''
        sValueMax = [c for c, v in dUnique.items() if v == max2]
        if len(sValueMax) > 0 and max2 != 0:
            mot2 = sValueMax[0]
        else:
            mot2 = ''
        sValueMax = [c for c, v in dUnique.items() if v == max3]
        if len(sValueMax) > 0 and max3 != 0:
            mot3 = sValueMax[0]
        else:
            mot3 = ''
        sValueMax = [c for c, v in dUnique.items() if v == max4]
        if len(sValueMax) > 0 and max4 != 0:
            mot4 = sValueMax[0]
        else:
            mot4 = ''
        sValueMax = [c for c, v in dUnique.items() if v == max5]
        if len(sValueMax) > 0 and max5 != 0:
            mot5 = sValueMax[0]
        else:
            mot5 = ''

        sDateClear = time.strftime("%D %H:%M", time.localtime(int(bt.sLastTimestamp[:-3])))

        #if len(mResult) > 0:
            #print('1 -> ' + str(mResult[max1]) + ' ||| 2 -> ' + str(mResult[max2]) + ' ||| 3 -> ' + str(mResult[max3]) + ' ||| 3 -> ' + str(mResult[max4]) + ' ||| 3 -> ' + str(mResult[max5]))
        print(sDateClear + '; ' + str(bt.nCurrentSize) + '; ' + mot1 + '; ' + mot2 + '; ' + mot3 + '; ' + mot4 + '; ' + mot5 + '; ')

        bt.next()
        i = 0
        nNbBatch += 1
        dUnique = {}
        #time.sleep(1000)
    #print(i)

def main():
    test()


    """handleEs = cHandleEs('test_app')
    es = handleEs.connectionToEs()
    bt = cBatchNumber(es, 'twitter2015', 'tweet', '1000', '1449493211')
    pt = bt.getPack()
"""
if __name__ == '__main__':
    main()

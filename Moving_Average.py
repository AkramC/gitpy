from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark= SparkSession.builder.appName("Moving Average").master("local").getOrCreate()

window=3

inputList=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]



def movingAverageold(index):

    if index < (len(inputList) -2 ):
        print(index )
        sum=( inputList[index] + inputList[index +1] + inputList[index +2] )/ window
    else:
        sum=0
    return sum
def movingAverage(listgroup):
    return int((listgroup[0]+listgroup[1]+listgroup[2])/window)

def averageList(partition) :
    partitionList=list(partition)
    groupList=[partitionList[n:n+window] for n in range(0, len(partitionList)-2)]


    groupListAverage=[]
    for smallList in groupList :
        sum=(smallList[0]+smallList[1]+smallList[2])/window
        groupListAverage.append(int(sum))

    return groupListAverage


def partitioindex(index,partition):
    partitionList = list(partition)
    groupList = [partitionList[n:n + window] for n in range(0, len(partitionList) - 2)]
    lastlist=groupList[len(groupList)-1]
    newList=lastlist[1:]
    if index != 3 :
        groupList.append(newList)





    return (groupList)

def getsplitList(inputList) :
    prevIndex=0
    currentIndex=0
    currentList=[]
    differentList=[]
    prevList =[]
    print(inputList)


    return differentList







def main():
    inputData=spark.sparkContext.parallelize(inputList,4)

    #inputDatagroup =inputData.mapPartitions(lambda input : {

    #list(input).map( lambda inputList : [inputList[n:n+window] for n in range(0, len(list(inputList))-2)]  ) })

    #inputDatagroup=inputData.mapPartitions( lambda inputListiter : averageList(inputListiter))

    inputDatagroup=inputData.mapPartitionsWithIndex(partitioindex)
    #inputgroup=inputDatagroup.groupByKey()
    getmiddleList = inputDatagroup.map(lambda x : getsplitList(x))




    print(inputDatagroup.collect())

if __name__ == '__main__':
    main()





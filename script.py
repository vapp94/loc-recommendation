import pandas as pd
import numpy as np
import math as m
import sys
import time
import random
from collections import OrderedDict

dfTestCaseDict = None
dfOneDict = None

dfTestCase = None
dfLocal = None
dfLocalUser = None
dfUser = None
dfLocalUserOneDay = None
dfUserOneDay = None
dfUsersCheckinLocal = None
dfListOfUsersFriends = None
dfFriendsFriendsSimilarity = None
dfFriendsLocalSimilarity = None
dfInverseLogFrequency = None
dfAllLocals = None

dfLocalDict = None
dfLocalUserDict = None
dfUserDict = None
dfCheckins = None
dfLocalUserOneDayDict = None
dfUserOneDayDict = None
dfUsersCheckinLocal = None
dfListOfUsersFriendsDict = None
dfFriendsFriendsSimilarityDict = None
dfFriendsLocalSimilarityDict = None
dfInverseLogFrequencyDict = None

allCheckins = 0

popChoice = 0  # it can be 0 for popM or 1 for popE
simChoice = 0 # it can be 0 for Mf, 1 for Mt and 2 for S

def checkEqualsOne(locId):
    if(locId in dfOneDict):
        return True
    else:
        return False

def openEqualsOne(inputEqualsOne):
    dfOne = pd.read_csv(inputEqualsOne, sep='\t')
    global dfOneDict
    dfOneDict = dfOne.set_index('location-id').to_dict(orient='index')
    print('Opened openEqualsOne')

def countTotalCheckinsByLocal(inputFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')

    dict = {}
    for index, row in df.iterrows():
        locId = row['location-id']
        infoId = str(row['user']) + '$' + str(row['check-in-time']) + '$' + str(row['location-id'])

        foundInEqualsOne = checkEqualsOne(locId)

        if (not foundInEqualsOne):
            if locId in dict.keys():
                dict[locId]+=1
            else:
                dict[locId]=1

    lenDict = len(dict)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['location-id', 'total-checkins']
    finalDf.set_index('location-id', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

    print('Done countTotalCheckinsByLocal')

def countTotalCheckinsByLocalByUser(inputFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')

    dict = {}
    for index, row in df.iterrows():
        locId = row['location-id']
        foundInEqualsOne = False
        infoId = str(row['user']) + '$' + str(row['check-in-time']) + '$' + str(row['location-id'])
        
        foundInEqualsOne = checkEqualsOne(locId)
        
        if (not foundInEqualsOne):
            userLocId = str(row['user']) + '$' + str(row['location-id'])
            if userLocId in dict.keys():
                dict[userLocId]+=1
            else:
                dict[userLocId]=1
    
    lenDict = len(dict)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['user-location-id', 'total-checkins']
    finalDf.set_index('user-location-id', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

    print('Done countTotalCheckinsByLocalByUser')

def countTotalCheckinsByUser(inputFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')

    dict = {}
    for index, row in df.iterrows():
        locId = row['location-id']
        foundInEqualsOne = False
        infoId = str(row['user']) + '$' + str(row['check-in-time']) + '$' + str(row['location-id'])

        foundInEqualsOne = checkEqualsOne(locId)
        
        if (not foundInEqualsOne):
            userId = str(row['user'])
            if userId in dict.keys():
                dict[userId]+=1
            else:
                dict[userId]=1
    
    lenDict = len(dict)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['user', 'total-checkins']
    finalDf.set_index('user', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

    print('Done countTotalCheckinsByUser')

def countTotalCheckinsByUserInOneDay(inputFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')

    dict = {}
    checkedInDates = {}
    for index, row in df.iterrows():
        locId = row['location-id']
        foundInEqualsOne = False
        infoId = str(row['user']) + '$' + str(row['check-in-time']) + '$' + str(row['location-id'])
        splitedDate = str(row['check-in-time'])[:10]

        foundInEqualsOne = checkEqualsOne(locId)

        if (not foundInEqualsOne):
            userId = str(row['user'])
            if userId in dict.keys():

                isDateInArray = False
                arrCheckedUser = checkedInDates[userId]
                for date in arrCheckedUser:
                    if splitedDate == date:
                        isDateInArray = True
                        break

                if (not isDateInArray):
                    checkedInDates[userId].append(splitedDate)
                    dict[userId]+=1
            else:
                checkedInDates[userId] = []
                checkedInDates[userId].append(splitedDate)
                dict[userId]=1
    
    lenDict = len(dict)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['user', 'total-days-checkins']
    finalDf.set_index('user', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

    print('Done countTotalCheckinsByUserInOneDay')

def countTotalCheckinsByLocalByUserInOneDay(inputFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')

    dict = {}
    checkedInDates = {}
    for index, row in df.iterrows():
        locId = row['location-id']
        foundInEqualsOne = False
        infoId = str(row['user']) + '$' + str(row['check-in-time']) + '$' + str(row['location-id'])
        splitedDate = str(row['check-in-time'])[:10]

        foundInEqualsOne = checkEqualsOne(locId)
        
        if (not foundInEqualsOne):
            userLocId = str(row['user']) + '$' + str(row['location-id'])
            locId = str(row['location-id'])
            if userLocId in dict.keys():

                isDateInArray = False
                arrCheckedUser = checkedInDates[userLocId]
                for date in arrCheckedUser:
                    if splitedDate == date:
                        isDateInArray = True
                        break

                if (not isDateInArray):
                    checkedInDates[userLocId].append(splitedDate)
                    dict[userLocId]+=1

            else:
                checkedInDates[userLocId] = []
                checkedInDates[userLocId].append(splitedDate)
                dict[userLocId]=1
    
    lenDict = len(dict)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['user-location-id', 'total-days-checkins']
    finalDf.set_index('user-location-id', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

    print('Done countTotalCheckinsByLocalByUserInOneDay')

def countTotalUsersCheckinsByLocal(inputFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')

    dict = {}
    checkedInUsers = {}
    for index, row in df.iterrows():
        locId = row['location-id']
        infoId = str(row['user']) + '$' + str(row['check-in-time']) + '$' + str(row['location-id'])
        userId = str(row['user'])

        foundInEqualsOne = checkEqualsOne(locId)

        if (not foundInEqualsOne):
            if locId in dict.keys():

                isUserInArray = False
                arrCheckedUser = checkedInUsers[locId]
                for uid in arrCheckedUser:
                    if userId == uid:
                        isUserInArray = True
                        break

                if (not isUserInArray):
                    checkedInUsers[locId].append(userId)
                    dict[locId]+=1
            else:
                checkedInUsers[locId] = []
                checkedInUsers[locId].append(userId)
                dict[locId]=1

    lenDict = len(dict)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['location-id', 'total-users-checkins']
    finalDf.set_index('location-id', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

    print('Done countTotalUsersCheckinsByLocal')

def getSimilarity(arr1, arr2):
    inter = 0
    len1 = len(arr1)
    len2 = len(arr2)
    i=0
    j=0
    while (i<len1) and (j<len2):
        val1 = arr1[i]
        val2 = arr2[j]
        if int(float(val1)) < int(float(val2)):
            i+=1
        elif int(float(val2)) < int(float(val1)):
            j+=1
        else:
            inter+=1
            i+=1
            j+=1
    
    similarity = (inter/min(len1, len2))

    return similarity

def getLocSimilarity(arr1, arr2):
    inter = 0
    len1 = len(arr1)
    len2 = len(arr2)
    for item1 in arr1:
        for item2 in arr2:
            if (item1 == item2):
                inter+=1
    
    similarity = (inter/min(len1, len2))
    
    return similarity

def getLogSimilarity(arr1, arr2):
    inter = 0
    len1 = len(arr1)
    len2 = len(arr2)
    arr = []
    for item1 in arr1:
        for item2 in arr2:
            if (item1 == item2):
                arr.append(item1)
    
    sum = 0
    for item in arr:
        sum += 1/(m.log(dfUsersCheckinLocalDict[item]['total-users-checkins']))

    return sum

def countFriendsFriendsSimilarity(inputFile, testCaseFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')
    dfTf = pd.read_csv(testCaseFile, sep='\t')
    
    testUsers = []
    for index, row in dfTf.iterrows():
        values = row['test-case-id'].split('$')
        userId = values[0]
        testUsers.append(userId)
    
    dict = {}
    otherUsers = []

    for index, row in df.iterrows():
        user1Id = str(row['user1'])
        user2Id = str(row['user2'])
        
        if user1Id in testUsers:
            otherUsers.append(user2Id)

    allUsers = testUsers + otherUsers
    allUsersNumber = list(map(int, allUsers))
    allUsersNumber.sort()
    allUsersSet = list(OrderedDict.fromkeys(allUsersNumber))
    print(allUsersSet)

    print('Done get necessary users.')
    
    for index, row in df.iterrows():
        user1Id = str(row['user1'])
        user2Id = str(row['user2'])
        
        if row['user1'] in allUsersSet:
            if user1Id in dict.keys():
                dict[user1Id].append(user2Id)
            else:
                dict[user1Id] = []
                dict[user1Id].append(user2Id)

    print('Done append a list for all necessary users.')

    dict2 = {}
    for user1, arr1 in dict.items():
        for user2, arr2 in dict.items():
            if (int(float(user2)) > int(float(user1))):
                similarity = getSimilarity(arr1, arr2)
                userUserId = user1+'$'+user2
                dict2[userUserId] = similarity
    
    lenDict = len(dict2)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict2:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict2[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['user-user-id', 'similarity']
    finalDf.set_index('user-user-id', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

    print('Done countFriendsFriendsSimilarity')

def countFriendsLocalSimilarity(inputFile, testCaseFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')
    dfTf = pd.read_csv(testCaseFile, sep='\t')
    
    testUsers = []
    for index, row in dfTf.iterrows():
        values = row['test-case-id'].split('$')
        userId = values[0]
        testUsers.append(userId)

    dict = {}
    for index, row in df.iterrows():
        userId = str(row['user'])
        locId = str(row['location-id'])

        if userId in dict.keys():
            arrLocs = dict[userId]
            
            isLocInArray = False
            for id in arrLocs:
                if id == locId:
                    isLocInArray = True
                    break

            if not isLocInArray:
                dict[userId].append(locId)
        else:
            dict[userId] = []
            dict[userId].append(locId)

    print('Done locations dict.')

    dict2 = {}
    for user1, arr1 in dict.items():
        if user1 in testUsers:
            for user2, arr2 in dict.items():
                if (int(float(user2)) > int(float(user1))):
                    userUserId = user1+'$'+user2
                    similarity = getLocSimilarity(arr1, arr2)
                    dict2[userUserId] = similarity
                elif (int(float(user1)) > int(float(user2))):
                    userUserId = user2+'$'+user1
                    similarity = getLocSimilarity(arr2, arr1)
                    dict2[userUserId] = similarity
            print('Done for ' + user1)
        
    
    lenDict = len(dict2)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict2:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict2[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['user-user-id', 'similarity']
    finalDf.set_index('user-user-id', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

    print('Done countFriendsLocalSimilarity')

def countInverseLogFrequency(inputFile, testCaseFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')
    dfTf = pd.read_csv(testCaseFile, sep='\t')
    
    testUsers = []
    for index, row in dfTf.iterrows():
        values = row['test-case-id'].split('$')
        userId = values[0]
        testUsers.append(userId)

    dict = {}
    for index, row in df.iterrows():
        userId = str(row['user'])
        locId = str(row['location-id'])

        if userId in dict.keys():
            arrLocs = dict[userId]
            
            isLocInArray = False
            for id in arrLocs:
                if id == locId:
                    isLocInArray = True
                    break

            if not isLocInArray:
                dict[userId].append(locId)
        else:
            dict[userId] = []
            dict[userId].append(locId)

    print('Done arr locs')

    dict2 = {}
    for user1, arr1 in dict.items():
        if user1 in testUsers:
            for user2, arr2 in dict.items():
                if (int(float(user2)) > int(float(user1))):
                    userUserId = user1+'$'+user2
                    similarity = getLogSimilarity(arr1, arr2)
                    dict2[userUserId] = similarity
                elif (int(float(user1)) > int(float(user2))):
                    userUserId = user2+'$'+user1
                    similarity = getLogSimilarity(arr2, arr1)
                    dict2[userUserId] = similarity
            print('Done for ' + user1)
    
    lenDict = len(dict2)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict2:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict2[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['user-user-id', 'similarity']
    finalDf.set_index('user-user-id', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')
    print('Done countInverseLogFrequency')

def listOfUsersFriends(inputFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')

    dict = {}
    for index, row in df.iterrows():
        user1Id = str(row['user1'])
        user2Id = str(row['user2'])

        if user1Id in dict.keys():
            dict[user1Id].append(user2Id)
        else:
            dict[user1Id] = []
            dict[user1Id].append(user2Id)
    
    lenDict = len(dict)
    w, h = 2, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict:
        matrix[currRow][0] = key
        matrix[currRow][1] = dict[key]
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['user', 'users']
    finalDf.set_index('user', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

    print('Done listOfUsersFriends')

def checkinsEqualsOne(inputFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')
    
    dict = {}
    for index, row in df.iterrows():
        locId = row['location-id']
        if locId in dict.keys():
            dict[locId]+=1
        else:
            dict[locId]=1

    countEqualsToOne = 0
    for key in dict:
        if dict[key] == 1:
            countEqualsToOne += 1

    lenDict = len(dict)
    w, h = 2, countEqualsToOne
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict:
        if dict[key] == 1:
            matrix[currRow][0] = key
            matrix[currRow][1] = dict[key]
            currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.columns = ['location-id', 'total-checkins']
    finalDf.set_index('location-id', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

def saveTestCases(inputFile, outputFile):
    df = pd.read_csv(inputFile, sep='\t')

    randomArr = random.sample(range(0, 500), 100)
    randomArr.sort()
    print(randomArr)

    dict = {}
    countNumberOfUsers = 0
    lastCheckin = None
    added = 0
    for index, row in df.iterrows():
        if countNumberOfUsers != row['user']:
            countNumberOfUsers += 1
            if (countNumberOfUsers-1) in randomArr:
                testCaseId = str(lastCheckin['user']) + '$' + str(lastCheckin['check-in-time']) + '$' + str(lastCheckin['location-id'])
                dict[testCaseId] = 0
                added += 1

        else:
            lastCheckin = row
        
        if added == 100:
            break

    lenDict = len(dict)
    w, h = 1, lenDict
    matrix = [[0 for x in range(w)] for y in range(h)]
    currRow = 0
    for key in dict:
        matrix[currRow][0] = key
        currRow += 1

    finalDf = pd.DataFrame(matrix)
    finalDf.drop(finalDf.index[len(finalDf)-1])
    finalDf.columns = ['test-case-id']
    finalDf.set_index('test-case-id', inplace=True)
    finalDf.to_csv(outputFile, sep='\t', encoding='utf-8')

def openCheckinsFile(inputFile):
    global dfCheckins
    dfCheckins = pd.read_csv(inputFile, sep='\t')

    print('Opened openCheckinsFile')


def openUsersCheckinLocalDict(inputTotalUsersCheckinLocal):
    global dfUsersCheckinLocal
    dfUsersCheckinLocal = pd.read_csv(inputTotalUsersCheckinLocal, sep='\t')
    print('Opened dfUsersCheckinLocal')

    global dfUsersCheckinLocalDict
    dfUsersCheckinLocalDict = dfUsersCheckinLocal.set_index('location-id').to_dict(orient='index')
    print('Put as dict dfUsersCheckinLocalDict')


def openFilesAsDict(
    inputLocal,
    inputLocalUser,
    inputUser,
    inputTestCase,
    inputTotalCheckinsLocalUserOneDay,
    inputTotalCheckinsUserOneDay,
    inputTotalUsersCheckinLocal,
    inputFriendsFriendsSimilarity,
    inputFriendsLocalSimilarity,
    inputInverseLogFrequency
    ):
    global dfLocal
    dfLocal = pd.read_csv(inputLocal, sep='\t')
    print('Opened dfLocal')
    
    global dfLocalUser
    dfLocalUser = pd.read_csv(inputLocalUser, sep='\t')
    print('Opened dfLocalUser')

    global dfUser
    dfUser = pd.read_csv(inputUser, sep='\t')
    print('Opened dfUser')

    global dfTestCase
    dfTestCase = pd.read_csv(inputTestCase, sep='\t')
    print('Opened dfTestCase')

    global dfLocalUserOneDay
    dfLocalUserOneDay = pd.read_csv(inputTotalCheckinsLocalUserOneDay, sep='\t')
    print('Opened dfLocalUserOneDay')

    global dfUserOneDay
    dfUserOneDay = pd.read_csv(inputTotalCheckinsUserOneDay, sep='\t')
    print('Opened dfUserOneDay')

    global dfUsersCheckinLocal
    dfUsersCheckinLocal = pd.read_csv(inputTotalUsersCheckinLocal, sep='\t')
    print('Opened dfUsersCheckinLocal')

    global dfFriendsFriendsSimilarity
    dfFriendsFriendsSimilarity = pd.read_csv(inputFriendsFriendsSimilarity, sep='\t')
    print('Opened dfFriendsFriendsSimilarity')

    global dfFriendsLocalSimilarity
    dfFriendsLocalSimilarity = pd.read_csv(inputFriendsLocalSimilarity, sep='\t')
    print('Opened dfFriendsLocalSimilarity')

    global dfInverseLogFrequency
    dfInverseLogFrequency = pd.read_csv(inputInverseLogFrequency, sep='\t')
    print('Opened dfInverseLogFrequency')

    global dfLocalDict
    dfLocalDict = dfLocal.set_index('location-id').to_dict(orient='index')
    print('Put as dict dfLocalDict')

    global dfLocalUserDict
    dfLocalUserDict = dfLocalUser.set_index('user-location-id').to_dict(orient='index')
    print('Put as dict dfLocalUserDict')

    global dfUserDict
    dfUserDict = dfUser.set_index('user').to_dict(orient='index')
    print('Put as dict dfUserDict')

    global dfTestCaseDict
    dfTestCaseDict = dfTestCase.set_index('test-case-id').to_dict(orient='index')
    print('Put as dict dfTestCaseDict')

    global dfLocalUserOneDayDict
    dfLocalUserOneDayDict = dfLocalUserOneDay.set_index('user-location-id').to_dict(orient='index')
    print('Put as dict dfLocalUserOneDayDict')

    global dfUserOneDayDict
    dfUserOneDayDict = dfUserOneDay.set_index('user').to_dict(orient='index')
    print('Put as dict dfUserOneDayDict')

    global dfUsersCheckinLocalDict
    dfUsersCheckinLocalDict = dfUsersCheckinLocal.set_index('location-id').to_dict(orient='index')
    print('Put as dict dfUsersCheckinLocalDict')

    if simChoice == 0:
        global dfFriendsFriendsSimilarityDict
        dfFriendsFriendsSimilarityDict = dfFriendsFriendsSimilarity.set_index('user-user-id').to_dict(orient='index')
        print('Put as dict dfFriendsFriendsSimilarityDict')
    elif simChoice == 1:
        global dfFriendsLocalSimilarityDict
        dfFriendsLocalSimilarityDict = dfFriendsLocalSimilarity.set_index('user-user-id').to_dict(orient='index')
        print('Put as dict dfFriendsLocalSimilarityDict')
    elif simChoice == 2:
        global dfInverseLogFrequencyDict
        dfInverseLogFrequencyDict = dfInverseLogFrequency.set_index('user-user-id').to_dict(orient='index')
        print('Put as dict dfInverseLogFrequencyDict')


def getPopM(locationId):
    popM = dfLocalDict[locationId]['total-checkins']/allCheckins
    return popM

def getPreE(locationId):
    popE = 0

    for key in dfUserDict:
        userLocId = str(key) + '$' + str(locationId)
        
        if userLocId in dfLocalUserDict:
            userLocCheckins = dfLocalUserDict[userLocId]['total-checkins']
            locCheckins = dfLocalDict[locationId]['total-checkins']
            div = userLocCheckins/locCheckins
            popE += (div)*(m.log(div))

    popE = -popE
    return popE

def getAbsDiffHours(checkinHour, checkinHour2):
    diff = abs(checkinHour-checkinHour2)
    if (diff <= 12):
        return diff
    else:
        if checkinHour < 12:
            checkinHour += 24
        elif checkinHour2 < 12:
            checkinHour2 += 24
        return (abs(checkinHour-checkinHour2))

def getPopExp(checkinTime, locationId):
    locationRows = dfCheckins.loc[dfCheckins['location-id'] == locationId]
    expSum = 0
    for row in locationRows.iterrows():
        userLocId = str(row[1]['user']) + '$' + str(row[1]['location-id'])
        userLocCheckins = dfLocalUserDict[userLocId]['total-checkins']
        checkinTimeRow = row[1]['check-in-time']
        checkinHour = checkinTimeRow[11] + checkinTimeRow[12]
        checkinHour2 = checkinTime[11] + checkinTime[12]
        absDiffHours = getAbsDiffHours(int(float(checkinHour)), int(float(checkinHour2)))
        expSum += (m.exp(-absDiffHours)*userLocCheckins)
    
    locCheckins = dfLocalDict[locationId]['total-checkins']
    popExp = expSum/locCheckins
    return popExp

def getLocationPopularity(checkinTime, locationId):
    choice = None
    global popChoice
    if popChoice == 0:
        choice = getPopM(locationId)
    else:
        choice = getPreE(locationId)
    
    popExp = getPopExp(checkinTime, locationId)
    
    lococationPopularity = popExp*choice
    return lococationPopularity

def getUserPreference(locationId, userId):
    infoId = userId + '$' + locationId

    totalCheckinsUserLoc = None
    try:
        totalCheckinsUserLoc = dfLocalUserDict[infoId]['total-checkins']
    except:
        totalCheckinsUserLoc = 0
    userIdInt = int(float(userId))
    totalCheckinsUser = dfUserDict[userIdInt]['total-checkins']

    totalCheckinsUserLocDay = None
    try:
        totalCheckinsUserLocDay = dfLocalUserOneDayDict[infoId]['total-days-checkins']
    except:
        totalCheckinsUserLocDay = 0
    totalCheckinsUserDay = dfUserOneDayDict[userIdInt]['total-days-checkins']
    totalCheckinsLoc = dfLocalDict[locationId]['total-checkins']
    totalUsersCheckinsLoc = dfUsersCheckinLocalDict[locationId]['total-users-checkins']
    
    return (totalCheckinsUserLoc/totalCheckinsUser)*(totalCheckinsUserLocDay/totalCheckinsUserDay)*(m.log(totalCheckinsLoc/totalUsersCheckinsLoc))

def getSocialInfluence(locationId, userId):
    friends = dfListOfUsersFriendsDict[userId]
    sum1 = 0
    sum2 = 0
    for f in friends:
        weight = 0
        userUserId = None
        
        if (int(float(userId)) < int(float(f))):
            userUserId = userId + '$' + f
        else:
            userUserId = f + '$' + userId
        global simChoice
        if simChoice == 0:
            if userUserId in dfFriendsFriendsSimilarityDict:
                weight = dfFriendsFriendsSimilarityDict[userUserId]['similarity']
            else:
                weight = 0
        elif simChoice == 1:
            if userUserId in dfFriendsLocalSimilarityDict:
                weight = dfFriendsLocalSimilarityDict[userUserId]['similarity']
            else:
                weight = 0
        else:
            if userUserId in dfInverseLogFrequencyDict:
                weight = dfInverseLogFrequencyDict[userUserId]['similarity']
            else:
                weight = 0

        userLocId = f + '$' + locationId
        if userLocId in dfLocalUserDict:
            sum1 += weight*dfLocalUserDict[userLocId]['total-checkins']
        
        sum2+=weight

    if sum1 == 0 or sum2 == 0:
        similarity = 0
    else:
        similarity = sum1/sum2

    return similarity        

def startCalculation(lpParam, upParam, siParam, N):
    testCase = 0
    maxPopularity = 0
    maxPreference = 0
    maxSocialInfluence = 0

    avgPopularity = 0
    avgPreference = 0
    avgSocialInfluence = 0
    total = 0
    totalPrecision = 0
    totalRecall = 0
    for key in dfTestCaseDict:
        values = key.split('$')
        userId = values[0]
        checkinTime = values[1]
        locationId = values[2]
        lps = []
        ups = []
        sis = []
        locsIds = []

        minLp = 9999999
        maxLp = -9999999
        minUp = 9999999
        maxUp = -9999999
        minSi = 9999999
        maxSi = -9999999
        for index, row in dfAllLocals.iterrows():
            locationTestId = str(row['location-id'])
            if locationTestId in dfOneDict:
                continue
            locationPopularity = getLocationPopularity(checkinTime, locationTestId)
            userPreference = getUserPreference(locationTestId, userId)
            socialInfluence = getSocialInfluence(locationTestId, userId)

            lps.append(locationPopularity)
            ups.append(userPreference)
            sis.append(socialInfluence)
            locsIds.append(locationTestId)

            if (locationPopularity < minLp):
                minLp = locationPopularity
            if (locationPopularity > maxLp):
                maxLp = locationPopularity

            if (userPreference < minUp):
                minUp = userPreference
            if (userPreference > maxUp):
                maxUp = userPreference

            if (socialInfluence < minSi):
                minSi = socialInfluence
            if (socialInfluence > maxSi):
                maxSi = socialInfluence

        for idx, val in enumerate(lps):
            if maxLp != 0:
                lpVal = (lps[idx]-minLp)/(maxLp-minLp)
            else:
                lpVal = 0

            if maxUp != 0:
                upVal = (ups[idx]-minUp)/(maxUp-minUp)
            else:
                upVal = 0

            if maxSi != 0:
                siVal = (sis[idx]-minSi)/(maxSi-minSi)
            else:
                siVal = 0
            
            locationTestId = locsIds[idx]

            score = lpParam*lpVal + upParam*upVal + siParam*siVal
            dfAllLocals.loc[dfAllLocals['location-id'] == locationTestId, 'score'] = score

        dfAllLocals.sort_values('score', axis=0, ascending=False, inplace=True)
        dfAllLocals.reset_index(drop=True)
        precision = 0
        recall = 0
        i = 0
        for index2, row2 in dfAllLocals.iterrows():
            currLocId = row2['location-id']
            
            if i == N:
                break
            
            if(currLocId == locationId):
                precision = 1/N
                recall = 1
                break

            i += 1

        totalPrecision += precision
        totalRecall += recall
        total += 1
        print("Done test case " + str(total))
    
    print('alfa = ' + str(siParam) + ' beta = ' + str(upParam) + ' N = ' + str(N))
    print('Average Precision: ' + str(totalPrecision/total) + ' Average Recall: ' + str(totalRecall/total))

def calculateNeededInfo(inputFileEdges, inputFileCheckins):
    global allCheckins
    allCheckins = dfLocal['total-checkins'].sum()

    df = pd.read_csv(inputFileEdges, sep='\t')

    global dfListOfUsersFriendsDict
    dfListOfUsersFriendsDict = {}
    for index, row in df.iterrows():
        user1Id = str(row['user1'])
        user2Id = str(row['user2'])

        if user1Id in dfListOfUsersFriendsDict.keys():
            dfListOfUsersFriendsDict[user1Id].append(user2Id)
        else:
            dfListOfUsersFriendsDict[user1Id] = []
            dfListOfUsersFriendsDict[user1Id].append(user2Id)

    print('Done dfListOfUsersFriendsDict')

    global dfAllLocals
    dfAllLocals = pd.read_csv(inputFileCheckins, sep='\t')
    del dfAllLocals['user']
    del dfAllLocals['check-in-time']
    del dfAllLocals['latitude']
    del dfAllLocals['longitude']
    dfAllLocals.drop_duplicates(subset=None, keep='first', inplace=True)
    dfAllLocals['score'] = 0
    dfAllLocals.reset_index(drop=True)
    print(len(dfAllLocals.index))

    print('Done dfAllLocals')


    


def saveTestCasesAndCheckinsEqualsOne():
    saveTestCases('testread.csv', 'testCases.csv')
    checkinsEqualsOne('testread.csv', 'checkinsEqualsOne.csv')

def openEqualsOneAndNeededCounts():
    openEqualsOne('checkinsEqualsOne.csv')
    countTotalCheckinsByLocal('testread.csv', 'countTotalCheckinsByLocal.csv')
    countTotalCheckinsByLocalByUser('testread.csv', 'countTotalCheckinsByLocalByUser.csv')
    countTotalCheckinsByUser('testread.csv', 'countTotalCheckinsByUser.csv')
    countTotalCheckinsByUserInOneDay('testread.csv', 'countTotalCheckinsByUserInOneDay.csv')
    countTotalCheckinsByLocalByUserInOneDay('testread.csv', 'countTotalCheckinsByLocalByUserInOneDay.csv')
    countTotalUsersCheckinsByLocal('testread.csv', 'countTotalUsersCheckinsByLocal.csv')
    countFriendsFriendsSimilarity('testreadedges.csv',
                               'testCases.csv',
                               'countFriendsFriendsSimilarity.csv'
                               )
    
    countFriendsLocalSimilarity('testread.csv',
                                'testCases.csv',
                                'countFriendsLocalSimilarity.csv')
    listOfUsersFriends('testreadedges.csv', 'listOfUsersFriends.csv')


def openFilesAndCountInverseLogFrequency():
    openCheckinsFile('testread.csv')
    openUsersCheckinLocalDict('countTotalUsersCheckinsByLocal.csv')
    countInverseLogFrequency('testread.csv',
                         'testCases.csv',
                         'countInverseLogFrequency.csv')


def doCalculation():
    openCheckinsFile('testread.csv')
    openFilesAsDict(
        'countTotalCheckinsByLocal.csv',
        'countTotalCheckinsByLocalByUser.csv',
        'countTotalCheckinsByUser.csv',
        'testCases.csv',
        'countTotalCheckinsByLocalByUserInOneDay.csv',
        'countTotalCheckinsByUserInOneDay.csv',
        'countTotalUsersCheckinsByLocal.csv',
        'countFriendsFriendsSimilarity.csv',
        'countFriendsLocalSimilarity.csv',
        'countInverseLogFrequency.csv'
    )
    
    calculateNeededInfo('testreadedges.csv', 'testread.csv')
    startCalculation(0.333, 0.333, 0.333, 1)
    startCalculation(0.333, 0.333, 0.333, 2)
    startCalculation(0.333, 0.333, 0.333, 5)
    startCalculation(0.333, 0.333, 0.333, 10)
    startCalculation(0.333, 0.333, 0.333, 20)

def doAll():
    saveTestCasesAndCheckinsEqualsOne()
    openEqualsOneAndNeededCounts()
    openFilesAndCountInverseLogFrequency()
    doCalculation()

doAll()
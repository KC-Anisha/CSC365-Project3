import copy
import json
from functools import reduce
import pandas as pd


def createUpdatedDataset():
    # Read 2020 and 2021 VAERS Data csv and combine them
    a = pd.read_csv("2020VAERSDATA.csv", encoding="ISO-8859-1", engine='python')
    b = pd.read_csv("2021VAERSDATA.csv", encoding="ISO-8859-1", engine='python')
    VAERSData = pd.concat([a, b], sort=False).drop_duplicates().reset_index(drop=True)

    # Read 2020 and 2021 VAERS Vax csv and combine them
    a = pd.read_csv("2020VAERSVAX.csv", encoding="ISO-8859-1", engine='python')
    b = pd.read_csv("2021VAERSVAX.csv", encoding="ISO-8859-1", engine='python')
    VAERSVax = pd.concat([a, b], sort=False).drop_duplicates(subset='VAERS_ID').reset_index(drop=True)
    # Remove all non Covid rows
    VAERSVax = VAERSVax[VAERSVax.VAX_TYPE.eq("COVID19")]

    # Read 2020 and 2021 VAERS Symptoms csv and combine them
    a = pd.read_csv("2020VAERSSYMPTOMS.csv", encoding="ISO-8859-1", engine='python')
    b = pd.read_csv("2021VAERSSYMPTOMS.csv", encoding="ISO-8859-1", engine='python')
    VAERSSymptoms = pd.concat([a, b], sort=False)

    # Combine all 3 datasets
    updatedData = reduce(lambda x, y: pd.merge(x, y, on='VAERS_ID', how='outer', sort=False),
                       [VAERSData, VAERSVax, VAERSSymptoms])
    # Remove all non Covid rows again
    updatedData = updatedData[updatedData.VAX_TYPE.eq("COVID19")]

    # Create a CSV file of the updated dataset
    updatedData.to_csv('VAERSDataNov15_21.csv', index=False)


def createHashMap(updatedDataset):
    jsonDataset = copy.deepcopy(updatedDataset)
    # Dictionary(HashMap) to store data
    hashMap = {}

    # Loop through the data and store it in a hashmap
    for row in jsonDataset:
        vaersId = row["VAERS_ID"]
        # If ID is already in the HashMap, just update the symptoms for it
        if vaersId in hashMap.keys():
            # Update the existing object with the additional symptoms
            obj = hashMap[vaersId]
            finalSymptoms = obj["Symptoms"]
            # Go through all symptoms and create a new symptoms array
            newSymptoms = []
            for x in range(0, 5):
                if row["SYMPTOM%d" % (x + 1)] is not None:
                    newSymptoms.append(json.loads(
                        '{ "SymptomName": "%s", "SymptomVersion": "%s"}' % (
                            row["SYMPTOM%d" % (x + 1)], row["SYMPTOMVERSION%d" % (x + 1)])))
            # Append the arrays and update the hashmap
            finalSymptoms.extend(newSymptoms)
            obj["Symptoms"] = finalSymptoms
            hashMap[vaersId] = obj
        # If ID is not in Hashmap, create a new entry
        else:
            # Create an array of symptoms by going through all symptoms row
            newSymptoms = []
            for x in range(0, 5):
                if row["SYMPTOM%d" % (x + 1)] is not None:
                    newSymptoms.append(json.loads(
                        '{ "SymptomName": "%s", "SymptomVersion": "%s"}' % (
                            row["SYMPTOM%d" % (x + 1)], row["SYMPTOMVERSION%d" % (x + 1)])))
            # Create a Covid Object with all the data
            for x in range(0, 5):
                del row['SYMPTOM%d' % (x + 1)]
                del row['SYMPTOMVERSION%d' % (x + 1)]
            obj = row
            obj["Symptoms"] = newSymptoms
            hashMap[vaersId] = obj

    return hashMap


def createTransactionDataset(datasetHashMap):
    hashMap = copy.deepcopy(datasetHashMap)
    jsonForTask1 = []
    for key in hashMap:
        # For every JSON object, we want to loop through the symptoms and create a more flattened JSON
        obj = hashMap[key]
        newObj = {}
        newObj["VAERS_ID"] = obj["VAERS_ID"]
        newObj["VAX_MANU"] = obj["VAX_MANU"]
        newObj["RECVDATE"] = obj["RECVDATE"]
        newObj["AGE_YRS"] = obj["AGE_YRS"]
        newObj["SEX"] = obj["SEX"]
        newObj["DIED"] = obj["DIED"]
        newObj["DATEDIED"] = obj["DATEDIED"]
        newObj["VAX_DATE"] = obj["VAX_DATE"]
        for count, symptom in enumerate(obj["Symptoms"], start=1):
            newObj["symptom_%d" % (count)] = symptom['SymptomName']
        newJson = json.loads(json.dumps(newObj))
        jsonForTask1.append(newJson)

    # Convert this JSON into a Dataset and return it
    return pd.DataFrame(jsonForTask1)


if __name__ == '__main__':
    # Create Updated Dataset CSV file
    print("Creating the updated data set and the CSV file ... ")
    createUpdatedDataset()

    # Task 1
    print("Task 1 - Creating a transaction dataset ... ")

    # Convert the data into a JSON format for easier looping
    updatedDataSetJSON = json.loads(pd.read_csv('VAERSDataNov15_21.csv').to_json(orient='records'))

    # Dictionary(HashMap) to store data
    datasetHashMap = createHashMap(updatedDataSetJSON)

    # Use hashMap to create the transaction dataset
    transactionDataset = createTransactionDataset(datasetHashMap)
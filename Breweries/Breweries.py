import requests
import json

def parsing(url): #Parses JSON files 
    response = requests.get(url) #gets the url
    text = json.dumps(response.json(), sort_keys=True, indent=4) #Cleans the output
    clean = json.loads(text) #decodes json file and returns a list
    return clean

def list():
    breweries = parsing("https://raw.githubusercontent.com/openbrewerydb/openbrewerydb/master/breweries.json") #Print the list of breweries from the API's dataset 
    x = 0
    while x <= len(breweries)-1: #iterates on all the brewerie names 
        print(breweries[x]['name']) #print the name by it's index - x is the index
        x += 1 #increment the index 

def searching ():
    name = input("Enter Brewerie Name: ") 
    search = parsing("https://api.openbrewerydb.org/breweries/search?query={0}".format(name))
    y = 0
    while y <= len(search)-1:
        print(search[y]['name'])
        y += 1

while True: #Infinite loop
    x = int(input("Enter (1) for a list of Breweies or (2) to search Breweries or (3) to exit: ")) 
    if x  == 1:
        list()
    elif x == 2:
        searching()
    elif x == 3:
        break      





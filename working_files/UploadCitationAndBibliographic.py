from rdflib import Graph, Literal, URIRef, RDF
import urllib.request
from abc import ABC, abstractmethod
from pandas import read_csv
import sqlite3   #I ADDED THE FOLLOWINGS
from sqlite3 import connect
import pandas as pd
import json

class Handler():
    def __init__(self):
        self.dbPathOrUrl = ""
    
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        try:
            self.dbPathOrUrl = pathOrUrl
            return True
        except Exception as e:
            return False



class UploadHandler(Handler):
    def __init__(self):
        super().__init__()
    
    @abstractmethod
    def PushDatatoDB(self, path: str) -> bool:
        pass
    

# Class CitationUploadHandler that inherits from UploadHandler and implements the PushDatatoDB method to read a CSV file, create RDF triples, and upload them to a SPARQL endpoint.
class CitationUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def PushDatatoDB(self, path: str) -> bool: #Takes the path of a CSV in input, transform the data in RDF triple and push them to a graph DB
        self.base_url = "https://schema.org/" #Set a base_url for RDF on the constructor
        my_graph = Graph() #create the graph

        #Classes Resources

        Identifier = URIRef(self.base_url + "identifier") #Not sure about this
        Citation = URIRef(self.base_url + "citation")
        Author_SC = URIRef(self.base_url + "author_sc")
        Journal_SC = URIRef(self.base_url + "journal_sc")

        #attributes

        timespan = URIRef(self.base_url + "timespan")
        creation = URIRef(self.base_url + "creation")

        #relations

        citing = URIRef(self.base_url + "hasCitingEntity")
        cited = URIRef(self.base_url + "hasCitedEntity")

        #Create the dataframe
        citations = read_csv(path, 
                        keep_default_na=False,
                        dtype={
                            "oci": "string",
                            "citing": "string",
                            "cited": "string",
                            "creation": "string",
                            "timespan": "string",
                            "journal_sc": "string",
                            "author_sc": "string"
                        })
    
        #to avoid mistake the method strip delete every blank space and avoid any syntax problem
        citations["oci"] = citations["oci"].str.strip()
        citations["citing"] = citations["citing"].str.strip()
        citations["cited"] = citations["cited"].str.strip()
        citations["creation"] = citations["creation"].str.strip()
        citations["timespan"] = citations["timespan"].str.strip()
        citations["journal_sc"] = citations["journal_sc"].str.strip().str.lower() == "true"
        citations["author_sc"] = citations["author_sc"].str.strip().str.lower() == "true"

        citations_local_id = {}

        #iterate all the rows in the Dataframe to transform them in RDF triple
        for idx, rows in citations.iterrows():

            subj = URIRef(self.base_url + rows["oci"])
            citations_local_id[idx] = subj
            my_graph.add((subj, RDF.type, Identifier))
            my_graph.add((subj, RDF.type, Citation))
            my_graph.add((subj, citing, URIRef(rows["citing"])))
            my_graph.add((subj, cited, URIRef(rows["cited"])))
            my_graph.add((subj, creation, Literal(rows["creation"])))
            my_graph.add((subj, timespan, Literal(rows["timespan"])))

            if rows["journal_sc"]: #define if a citation include a journal self citation
                my_graph.add((subj, RDF.type, Journal_SC))
            
            if rows["author_sc"]: #define if a citation include a author self citation
                my_graph.add((subj, RDF.type, Author_SC))
        
        endpoint = self.getDbPathOrUrl() #set the endpoint using the method of the superclass

        rdf_data = my_graph.serialize(format="nt").encode("utf-8") #serialize all the data in nt format and encode it in UTF-8

        req = urllib.request.Request( #create a request to the server and establish that we are sending a n-triples file
            endpoint,
            data=rdf_data,
            headers={'Content-Type': 'application/n-triples'}
        )

        try: # try the code to catch the Exception
            with urllib.request.urlopen(req) as response:
                if response.status == 200:
                    check = True
                else:
                    print(f"Failed to upload data. Status code: {response.status}")
                    check = False
        except Exception as e:
            print(f"Error occurred: {e}")
            check = False

        return check

class BibliographicEntityUploadHandler(UploadHandler): # BibliographicEntityUploadHandler inherits from UploadHandler and implements the method PushDataToDB that reads a .json file and creates the relational db (tables) for querying the db through SQL.
    def __init__(self):
        super().__init__()

    def PushDatatoDB(self, path: str) -> bool: #Takes in input a path for a json file and creates a relational db
        try: #Check if the file is not corrupted or missing. There should be an "except" block later.
            with open(path, "r", encoding="utf-8") as f: #Open the file and close it automatically even if there are errors.
                raw_data = json.load(f) #Transform the json file into a Python object (a list of dictionaries).

            df = pd.json_normalize(raw_data) #Create the DataFrame (if there are nested elements, this flattens them).

            df["internal_id"] = ["internal_" + str(i) for i in range(len(df))] #Create the internal ID
            
            #Create the tables
            bibliographic_entity = df[["internal_id", "title", "pub_date", "venue"]]
            authors_table = df[["internal_id", "author"]].explode("author")
            id_table = df[["internal_id", "id"]].explode("id")

            db_path = self.getDbPathOrUrl() #Save the path in order to retrieve it later.

            with sqlite3.connect(db_path) as conn: #The "with" closes the command even with errors. Open the SQLite database in db_path or create a new one if needed.
                bibliographic_entity.to_sql("BibliographicEntity", conn, if_exists="replace", index=False) #Manage the tables in SQL
                authors_table.to_sql("BibliographicEntity_Authors", conn, if_exists="replace", index=False)
                id_table.to_sql("BibliographicEntity_ID", conn, if_exists="replace", index=False)
            return True
        except Exception as e: #Check if there are any errors in the "try" block.
            print(f"Error during upload: {e}")
            return False
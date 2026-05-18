from rdflib import Graph, Literal, URIRef, RDF
import urllib.request
from abc import abstractmethod
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
    def pushDataToDb(self, path: str) -> bool:
        pass
    

# Class CitationUploadHandler that inherits from UploadHandler and implements the PushDatatoDB method to read a CSV file, create RDF triples, and upload them to a SPARQL endpoint.
class CitationUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path: str) -> bool: #Takes the path of a CSV in input, transform the data in RDF triple and push them to a graph DB
        self.base_url = "https://schema.org/" #Set a base_url for RDF on the constructor
        my_graph = Graph() #create the graph

        #Classes Resources

        Identifier = URIRef(self.base_url + "identifier") #Not sure about this
        Citation = URIRef(self.base_url + "citation")
        Author_SC = URIRef(self.base_url + "author_sc")
        Journal_SC = URIRef(self.base_url + "journal_sc")

        #attributes of the resources

        timespan = URIRef(self.base_url + "timespan")
        creation = URIRef(self.base_url + "creation")

        #relations between the resources

        citing = URIRef(self.base_url + "hasCitingEntity")
        cited = URIRef(self.base_url + "hasCitedEntity")

        #Create the dataframe and for every row i define a type
        citations = read_csv(path, 
                        keep_default_na=False,
                        dtype={
                            "oci": str,
                            "citing": str,
                            "cited": str,
                            "creation": str,
                            "timespan": str,
                            "journal_sc": str,
                            "author_sc": str
                        })
    
        #to avoid mistake the method strip delete every blank space and avoid any syntax problem
        citations["oci"] = citations["oci"].str.strip()
        citations["citing"] = citations["citing"].str.strip()
        citations["cited"] = citations["cited"].str.strip()
        citations["creation"] = citations["creation"].str.strip()
        citations["timespan"] = citations["timespan"].str.strip()
        citations["journal_sc"] = citations["journal_sc"].str.strip().str.lower() 
        citations["author_sc"] = citations["author_sc"].str.strip().str.lower()

        #iterate all the rows in the Dataframe to transform them in RDF triple adding to the graph the type of the resource and the relations between them. If there is a journal self citation or an author self citation, I add to the graph also this information.
        for idx, rows in citations.iterrows():
            subj = URIRef(self.base_url + rows["oci"]) #the subject of the triple is the oci, which is a unique identifier for each citation. I create a URIRef using the base_url and the oci value to ensure that each citation has a unique and consistent identifier in the RDF graph.
            my_graph.add((subj, RDF.type, Identifier)) #I add to the graph the type of the resource, which is "Identifier". This indicates that the subject of the triple is an identifier for a citation. By defining this type, we can later query the graph to retrieve all resources that are of type "Identifier", which will allow us to easily access and analyze the citation data.
            my_graph.add((subj, RDF.type, Citation)) #I add to the graph the type of the resource, which is "Citation". This indicates that the subject of the triple is a citation. By defining this type, we can later query the graph to retrieve all resources that are of type "Citation", which will allow us to easily access and analyze the citation data.
            my_graph.add((subj, citing, URIRef(rows["citing"]))) #I add to the graph the relation "hasCitingEntity" between the subject (the citation) and the citing entity (the paper that is citing). I create a URIRef for the citing entity using the value from the "citing" column in the CSV file. This allows us to represent the relationship between the citation and the citing entity in the RDF graph, which can be useful for querying and analyzing citation patterns.
            my_graph.add((subj, cited, URIRef(rows["cited"]))) #I add to the graph the relation "hasCitedEntity" between the subject (the citation) and the cited entity (the paper that is being cited). I create a URIRef for the cited entity using the value from the "cited" column in the CSV file. This allows us to represent the relationship between the citation and the cited entity in the RDF graph, which can be useful for querying and analyzing citation patterns.
            my_graph.add((subj, creation, Literal(rows["creation"])))#I add to the graph the relation "creation" between the subject (the citation) and the creation date (the date when the citation was created). I create a Literal for the creation date using the value from the "creation" column in the CSV file. This allows us to represent the temporal aspect of the citation in the RDF graph, which can be useful for querying and analyzing citation trends over time.
            my_graph.add((subj, timespan, Literal(rows["timespan"])))#I add to the graph the relation "timespan" between the subject (the citation) and the timespan (the time between the publication of the citing paper and the cited paper). I create a Literal for the timespan using the value from the "timespan" column in the CSV file. This allows us to represent the temporal aspect of the citation in the RDF graph, which can be useful for querying and analyzing citation patterns over different time periods.
            if rows["journal_sc"] == "yes": #define if a citation include a journal self citation
                my_graph.add((subj, RDF.type, Journal_SC)) #I add to the graph the type of the resource, which is "Journal_SC". This indicates that the subject of the triple is a journal self-citation. By defining this type, we can later query the graph to retrieve all resources that are of type "Journal_SC", which will allow us to easily access and analyze journal self-citation patterns in the citation data.
            if rows["author_sc"] == "yes": #define if a citation include a author self citation
                my_graph.add((subj, RDF.type, Author_SC))#I add to the graph the type of the resource, which is "Author_SC". This indicates that the subject of the triple is an author self-citation. By defining this type, we can later query the graph to retrieve all resources that are of type "Author_SC", which will allow us to easily access and analyze author self-citation patterns in the citation data.
        
        endpoint = self.getDbPathOrUrl() #set the endpoint using the method of the superclass

        rdf_data = my_graph.serialize(format="nt").encode("utf-8") #serialize all the data in nt format and encode it in UTF-8

        req = urllib.request.Request( #create a request to the server and establish that we are sending a n-triples file. The "data" parameter is used to send the RDF data in the body of the request, and the "headers" parameter is used to specify the content type of the data being sent. The "method" parameter is set to "POST" to indicate that we want to create new data on the server.
            endpoint,
            data=rdf_data,
            method="POST",
            headers={'Content-Type': 'application/n-triples'} #data are been sending in a one packet and not one by one, so the content type is application/n-triples and not text/plain. This helps to make the upload more efficient and faster, especially for large datasets.
        )

        try: # try used to check if the server is working and if the data is correctly uploaded. If there is an error, it will be caught in the except block and printed.
            with urllib.request.urlopen(req) as response: # open the connection and send the data to the server. The "with" statement ensures that the connection is properly closed after the block is executed, even if an error occurs.
                if response.status == 200: # check if the response status is 200, which indicates that the data was successfully uploaded. If the status is not 200, it means that there was an error in uploading the data, and we print the status code for debugging purposes.
                    check = True
                else:
                    print(f"Failed to upload data. Status code: {response.status}") # print the status code for debugging purposes
                    check = False
        except Exception as e: # catch any exceptions that occur during the upload process, such as network errors or server issues. If an exception is caught, we print a fatal error message along with the exception details for debugging purposes, and return False to indicate that the upload was unsuccessful.
            print(f"\nFatal Error in loading data: {e}\n")
            return False

        return check

class BibliographicEntityUploadHandler(UploadHandler): # BibliographicEntityUploadHandler inherits from UploadHandler and implements the method PushDataToDB that reads a .json file and creates the relational db (tables) for querying the db through SQL.
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path: str) -> bool: #Takes in input a path for a json file and creates a relational db
        try: #Check if the file is not corrupted or missing. There should be an "except" block later.
            with open(path, "r", encoding="utf-8") as f: #Open the file and close it automatically even if there are errors.
                raw_data = json.load(f) #Transform the json file into a Python object (a list of dictionaries).

            df = pd.json_normalize(raw_data) #Create the DataFrame (if there are nested elements, this flattens them).

            df["internal_id"] = ["internal_" + str(i) for i in range(len(df))] #Create the internal ID

            #SUSANNA 1. Riempi i NaN nel DataFrame principale prima di estrarre le tabelle
            df = df.fillna("")
            
            #Create the tables
            bibliographic_entity = df[["internal_id", "title", "pub_date", "venue"]]

            # SUSANNA HA AGGIUNTO .fillna("") alla fine 2. Per le tabelle con gli explode, usa fillna("") DOPO l'explode,
            # perché l'explode di una lista vuota genera un NaN.
            authors_table = df[["internal_id", "author"]].explode("author").fillna("")
            id_table = df[["internal_id", "id"]].explode("id").fillna("")

            db_path = self.getDbPathOrUrl() #Save the path in order to retrieve it later.

            with sqlite3.connect(db_path) as conn: #The "with" closes the command even with errors. Open the SQLite database in db_path or create a new one if needed.
                bibliographic_entity.to_sql("BibliographicEntity", conn, if_exists="replace", index=False) #Manage the tables in SQL
                authors_table.to_sql("BibliographicEntity_Authors", conn, if_exists="replace", index=False)
                id_table.to_sql("BibliographicEntity_ID", conn, if_exists="replace", index=False)
            return True
        except Exception as e: #Check if there are any errors in the "try" block.
            print(f"Error during upload: {e}")
            return False
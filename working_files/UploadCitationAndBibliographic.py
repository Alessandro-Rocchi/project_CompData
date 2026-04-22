from rdflib import Graph, Literal, URIRef, RDF
import urllib.request
from pandas import read_csv


class UploadHandler():
    def __init__(self, dbPathorURL: str):
        self.dbPathorURL = dbPathorURL

    def PushDatatoDB(self, path: str) -> bool:
        pass
    

# Class CitationUploadHandler that inherits from UploadHandler and implements the PushDatatoDB method to read a CSV file, create RDF triples, and upload them to a SPARQL endpoint.
class CitationUploadHandler(UploadHandler):

    # Initializer of the Class
    def __init__(self, dbPathorURL: str):
        super().__init__(dbPathorURL) #Recall the attribute dbPathorURL from the superclass
        self.base_url = "https://schema.org/" #Set a base_url for RDF on the constructor

    def PushDatatoDB(self, path: str) -> bool: #Takes the path of a CSV in input, transform the data in RDF triple and push them to a graph DB
        
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
        
        endpoint = "http://localhost:3030/mioprogetto/data" #set the endpoint using the method of the superclass

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
                    print("Data uploaded successfully.")
                else:
                    print(f"Failed to upload data. Status code: {response.status}")
                    check = False
        except Exception as e:
            print(f"Error occurred: {e}")
            check = False

        return check

uploadHandler = CitationUploadHandler("http://localhost:3030/mioprogetto/data")

print(uploadHandler.PushDatatoDB("data/dh_citations.csv"))
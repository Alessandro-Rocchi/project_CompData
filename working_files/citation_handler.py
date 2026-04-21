from rdflib import Graph, Literal, URIRef, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import urllib.request
from pandas import read_csv


class CitationUploadHandler:
    def __init__(self):
        self.graph = Graph()

    def PushDatatoDB(self, path: str)-> bool:
        
        my_graph = Graph()

        base_url = "https://schema.org/"

        #Classes Resources
        Identifier = URIRef(base_url + "identifier")
        Citation = URIRef(base_url + "citation")
        Author_SC = URIRef(base_url + "author_sc")
        Journal_SC = URIRef(base_url + "journal_sc")

        #attributes

        timespan = URIRef(base_url + "timespan")
        creation = URIRef(base_url + "creation")

        #relations

        citing = URIRef(base_url + "hasCitingEntity")
        cited = URIRef(base_url + "hasCitedEntity")


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

        citations.columns = citations.columns.str.strip()
        citations["oci"] = citations["oci"].str.strip()
        citations["citing"] = citations["citing"].str.strip()
        citations["cited"] = citations["cited"].str.strip()

        for rows in citations.iterrows():

            subj = URIRef(base_url + rows["oci"])    
            my_graph.add((subj, RDF.type, Citation))
            my_graph.add((subj, citing, URIRef(rows["citing"])))
            my_graph.add((subj, cited, URIRef(rows["cited"])))
            my_graph.add((subj, creation, Literal(rows["creation"])))
            my_graph.add((subj, timespan, Literal(rows["timespan"])))
            if rows["journal_sc"]:
                my_graph.add((subj, RDF.type, Journal_SC))
            elif rows["author_sc"]:
                my_graph.add((subj, RDF.type, Author_SC))


        print("-- Number of triples added to the graph after processing the venues")
        print(len(my_graph))

        print("Preparazione dell'invio massivo a Blazegraph...")

        endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

        rdf_data = my_graph.serialize(format="nt").encode("utf-8")

        req = urllib.request.Request(
            endpoint,
            data=rdf_data,
            headers={'Content-Type': 'text/plain'} # text/plain è il formato esatto per N-Triples
        )

        try:
            with urllib.request.urlopen(req) as response:
                if response.status == 200:
                    print("Connessione chiusa. Caricamento completato con successo in pochi secondi!")
                    check = True
                else:
                    print(f"Errore anomalo. Status Code: {response.status}")
                    check = False
        except Exception as e:
            print(f"Si è verificato un errore di rete durante l'invio: {e}")
            check = False
        return check
        
from rdflib import Graph, Literal, URIRef, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pandas import Series, read_csv

my_graph = Graph()

base_url = "https://schema.org/"

#Classes Resources

Citation = URIRef(base_url + "citation")
Author_SC = URIRef(base_url + "author_sc")
Journal_SC = URIRef(base_url + "journal_sc")

#attributes

timespan = URIRef(base_url + "timespan")
creation = URIRef(base_url + "creation")

#relations

citing = URIRef(base_url + "hasCitingEntity")
cited = URIRef(base_url + "hasCitedEntity")


citations = read_csv("data/dh_citations.csv", 
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
citations["citing"] = citations["citing"].str.strip()
citations["cited"] = citations["cited"].str.strip()

citation_internal_oci = {}

for idx, rows in citations.iterrows():
    citation_internal_id = "citation_" + str(idx)
    subj = URIRef(base_url + citation_internal_id)
    my_graph.add((subj, RDF.type, Citation))
    my_graph.add((subj, citing, URIRef(rows["citing"])))
    my_graph.add((subj, cited, URIRef(rows["cited"])))
    my_graph.add((subj, creation, Literal(rows["creation"])))
    my_graph.add((subj, timespan, Literal(rows["timespan"])))

    if rows["journal_sc"]:
        my_graph.add((subj, Journal_SC, Literal(rows["journal_sc"])))
    elif rows["author_sc"]:
        my_graph.add((subj, Author_SC, Literal(rows["author_sc"])))


print("-- Number of triples added to the graph after processing the venues")
print(len(my_graph))

store = SPARQLUpdateStore()

endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

store.open((endpoint, endpoint))

for triple in my_graph.triples((None, None, None)):
   store.add(triple)

store.close()

print("connessione chiusa")
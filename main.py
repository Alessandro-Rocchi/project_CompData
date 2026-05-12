from working_files.uploadCitationAndBibliographic import CitationUploadHandler, BibliographicEntityUploadHandler
from working_files.queryHandler import BibliographicEntityQueryHandler, CitationQueryHandler
from working_files.basicAndFullQueryEngine import BasicQueryEngine


def main():
    CitUp = CitationUploadHandler()
    BibUp = BibliographicEntityUploadHandler()
    CitUp.setDbPathOrUrl("http://172.20.10.2:9999/blazegraph/sparql") # Current Blazegraph endpoint URL
    BibUp.setDbPathOrUrl("data/try_sql.db")
    CitUp.PushDatatoDB("data/dh_citations.csv")
    BibUp.PushDatatoDB("data/dh_metadata.json")
    QCit = CitationQueryHandler()
    QCit.setDbPathOrUrl("http://172.20.10.2:9999/blazegraph/sparql")
    QBib = BibliographicEntityQueryHandler()
    QBib.setDbPathOrUrl("data/try_sql.db")
    q1 = BasicQueryEngine()
    q1.addBibliographicEntityHandler(QBib)
    q_list = q1.getBibliographicEntitiesWithinDate("2018-08-01", "2018-08-31")
    print(q_list[0].title)
if __name__ == "__main__":
    main()

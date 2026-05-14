from working_files.uploadCitationAndBibliographic import CitationUploadHandler, BibliographicEntityUploadHandler
from working_files.queryHandler import BibliographicEntityQueryHandler, CitationQueryHandler
from working_files.basicAndFullQueryEngine import BasicQueryEngine


def main():
    CitUp = CitationUploadHandler()
    BibUp = BibliographicEntityUploadHandler()
    CitUp.setDbPathOrUrl("http://10.201.14.213:9999/blazegraph/sparql") # Current Blazegraph endpoint URL
    BibUp.setDbPathOrUrl("data/try_sql.db")
    #CitUp.PushDatatoDB("data/dh_citations.csv")
    #BibUp.PushDatatoDB("data/dh_metadata.json")

    QCit = CitationQueryHandler()
    QCit.setDbPathOrUrl("http://10.201.14.213:9999/blazegraph/sparql")
    QBib = BibliographicEntityQueryHandler()
    QBib.setDbPathOrUrl("data/try_sql.db")

    q1 = BasicQueryEngine()
    q1.addBibliographicEntityHandler(QBib)
    q_list = q1.getBibliographicEntitiesWithinDate("2018-08-01", "2018-08-31")
    print(q_list[0].title)
    
    # Testing citations methods of basic query engine

    # q1 = BasicQueryEngine()
    # q1.addCitationHandler(QCit)
    
    # print("Testing getCitationsWithinDate...")
    # citations = q1.getCitationsWithinDate("2020", "2020")
    # print(len(citations))

    # if citations:
    #     print(type(citations[0]))
    #     print(citations[0].getIds())
    #     print(citations[0].getCreation())
    #     print(citations[0].getTimespan())
    # else:
    #     print("No citations found in this date range.")


    
if __name__ == "__main__":
    main()

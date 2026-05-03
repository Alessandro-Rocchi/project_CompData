from working_files.UploadCitationAndBibliographic import CitationUploadHandler, BibliographicEntityUploadHandler
from working_files.queryHandler import BibliographicEntityQueryHandler, CitationQueryHandler


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
    
    id = QBib.getById("omid:br/0604944107")
    print("By ID:")
    for idx, row in id.iterrows():
        print("\nThe index of the current row is", idx)
        print("The content of the row is as follows:")
        print(row)
    


    # Test di getCitationsWithinTimespan con min_timespan e max_timespan entrambi specificati
    # df = QCit.getCitationsWithinTimespan("P1Y", "P5Y")
    # print(df)

    # Test di getCitationsWithinDate con min_date e max_date entrambi specificati
    # df = QCit.getCitationsWithinDate("2020", "2020")
    # print(df[["citation_id", "creation", "timespan"]].head(30))
    # print("Number of results:", len(df))





    #title = QBib.getBibliographicEntitiesWithTitle("Digital Cultural Strategies Within The Context Of Digital Humanities Economics")
    #print("\nBy Title:")
    #for idx, row in title.iterrows():
    #    print("\nThe index of the current row is", idx)
    #    print("The content of the row is as follows:")
    #    print(row)

    #author = QBib.getBibliographicEntitiesWithAuthor("Frings-Hessami, Viviane")
    #print("\nAuthor:")
    #for idx, row in author.iterrows():
    #    print("\nThe index of the current row is", idx)
    #    print("The content of the row is as follows:")
    #    print(row)
    
    #date = QBib.getBibliographicEntitiesWithinPublicationDate("1989-06", "2013-08")
    #print("\nCitations within timespan:")
    #for idx, row in date.iterrows():
    #    print("\nThe index of the current row is", idx)
    #    print("The content of the row is as follows:")
    #    print(row)

if __name__ == "__main__":
    main()

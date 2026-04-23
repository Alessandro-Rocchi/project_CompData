from UploadCitationAndBibliographic import CitationUploadHandler, BibliographicUploadHandler

class QueryHandler(): # fatto da solo da Copilot
    def __init__(self, dbPathorURL: str):
        self.dbPathorURL = dbPathorURL

    def QueryDB(self, query: str) -> list:
        pass

class CitationQueryHandler(QueryHandler):
    def __init__(self, dbPathorURL: str): # fatto da solo da Copilot
        super().__init__(dbPathorURL)

    # fatto da solo da Copilot:
    # def QueryDB(self, query: str) -> list:
    #     # Here you would implement the logic to execute the SPARQL query against the graph database
    #     # and return the results as a list. This is a placeholder implementation.
    #     results = []  # This should be replaced with actual query results
    #     return results
    
    # metodi da implementare per citazioni:
    # getAllCitations()
    # gelAllAuthorSelfCitations()
    # getAllJournalSelfCitations()
    # getCitationsWithinTimespan(min_timespan, max_timespan)
    # getCitationsWithinDate(start_date, end_date)

class BibliographicQueryHandler(QueryHandler):
    def __init__(self, dbPathorURL: str): # fatto da solo da Copilot
        super().__init__(dbPathorURL)

    # fatto da solo da Copilot:
    # def QueryDB(self, query: str) -> list:
    #     # Here you would implement the logic to execute the SPARQL query against the graph database
    #     # and return the results as a list. This is a placeholder implementation.
    #     results = []  # This should be replaced with actual query results
    #     return results
    
    # metodi da implementare per bibliografia:
    # getAllBibliographicEntities()
    # getBibliographicEntitiesWithTitle(title)
    # getBibliographicEntitiesWithAuthor(author)
    # getBibliographicEntitiesWithinPublicationDate(start_date, end_date)
    # getBibliographicEntitiesWithVenue(venue)
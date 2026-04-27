from UploadCitationAndBibliographic import CitationUploadHandler, BibliographicEntityUploadHandler, Handler
import sqlite3
import pandas as pd

class QueryHandler(Handler): # fatto da solo da Copilot
    def __init__(self):
        super().__init__()

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

class BibliographicEntityQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getById(self, id_string: str) -> pd.DataFrame:
        
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            query = """
                SELECT BibliographicEntity.* FROM BibliographicEntity 
                JOIN BibliographicEntity_ID ON BibliographicEntity.internal_id = BibliographicEntity_ID.internal_id 
                WHERE BibliographicEntity_ID.id = ?
            """
            return pd.read_sql(query, con, params=(id_string,))

    def getAllBibliographicEntities(self) -> pd.DataFrame:
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            return pd.read_sql("SELECT * FROM BibliographicEntity", con)

    def getBibliographicEntitiesWithTitle(self, title: str) -> pd.DataFrame:
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            query = "SELECT * FROM BibliographicEntity WHERE title LIKE ?"
            return pd.read_sql(query, con, params=(f"%{title}%",))

    def getBibliographicEntitiesWithAuthor(self, author_name: str) -> pd.DataFrame:
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            query = """
                SELECT BibliographicEntity.* FROM BibliographicEntity 
                JOIN BibliographicEntity_Authors ON BibliographicEntity.internal_id = BibliographicEntity_Authors.internal_id 
                WHERE BibliographicEntity_Authors.author LIKE ?
            """
            return pd.read_sql(query, con, params=(f"%{author_name}%",))

    def getBibliographicEntitiesWithinPublicationDate(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            query = "SELECT * FROM BibliographicEntity WHERE 1=1"
            params = []
            if start_date:
                query += " AND pub_date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND pub_date <= ?"
                params.append(end_date)
            return pd.read_sql(query, con, params=params)

    def getBibliographicEntitiesWithVenue(self, venue_name: str) -> pd.DataFrame:
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            query = "SELECT * FROM BibliographicEntity WHERE venue LIKE ?"
            return pd.read_sql(query, con, params=(f"%{venue_name}%",))
from .UploadCitationAndBibliographic import Handler
from sparql_dataframe import get
import sqlite3
import pandas as pd

class QueryHandler(Handler): # fatto da solo da Copilot
    def __init__(self):
        super().__init__()

    def QueryDB(self, query: str) -> list:
        pass

class CitationQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    # Takes a SPARQL query as input and returns the result as a pandas DataFrame
    def QueryDB(self, query: str) -> pd.DataFrame: 
        return get(self.getDbPathOrUrl(), query, True)

    # Returns all citations in the database
    def getAllCitations(self) -> pd.DataFrame: 

        # For every resource ?citation_id that is a citation, return: 
        # the entity it cites from, the entity it cites, its creation date, and its timespan.
        query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
            PREFIX schema: <https://schema.org/> 

            SELECT ?citation_id ?citing ?cited ?creation ?timespan 
            WHERE {
                ?citation_id rdf:type schema:citation .
                ?citation_id schema:hasCitingEntity ?citing . 
                ?citation_id schema:hasCitedEntity ?cited .
                ?citation_id schema:creation ?creation .
                ?citation_id schema:timespan ?timespan .
            }
        """
    
        return self.QueryDB(query)
    
    # # Returns all citations where the citing and cited entities share at least one author
    # def getAllAuthorSelfCitations(self) -> pd.DataFrame: 

    # SELECT ?id ?citing ?cited ?creation ?timespan
    # WHERE {
    #     ?id rdf:type schema:citation .
    #     ?id rdf:type schema:author_sc .
    #     ?id schema:hasCitingEntity ?citing .
    #     ?id schema:hasCitedEntity ?cited .
    #     ?id schema:creation ?creation .
    #     ?id schema:timespan ?timespan .
    # }
    # """
    
    #     return self.QueryDB(query)
    
    # metodi da implementare per citazioni:
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
from .uploadCitationAndBibliographic import Handler
from sparql_dataframe import get
import sqlite3  
import pandas as pd
from abc import ABC, abstractmethod

class QueryHandler(Handler, ABC): 
    def __init__(self):
        super().__init__()

    @abstractmethod
    def getById(self, id: str) -> pd.DataFrame:
        pass

class CitationQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    # Takes a SPARQL query as input and returns the result as a pandas DataFrame
    # ex QueryDB, changed name and made it private since it's a helper method 
    def _runSparqlQuery(self, query: str) -> pd.DataFrame: 
        return get(self.getDbPathOrUrl(), query, True)
    
    # Returns the citation with the specified ID
    def getById(self, id_string: str) -> pd.DataFrame:

        # Use id_string if it's a full URI; if not construct the full URI.
        if id_string.startswith("http://") or id_string.startswith("https://"):
            citation_uri = id_string
        else:
            citation_uri = "https://schema.org/" + id_string


        # For the resource with the specified URI, return:
        # its citing entity, cited entity, creation date, and timespan.
        query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
            PREFIX schema: <https://schema.org/> 

            SELECT ?citation_id ?citing ?cited ?creation ?timespan 
            WHERE {{
                ?citation_id rdf:type schema:citation .
                ?citation_id schema:hasCitingEntity ?citing . 
                ?citation_id schema:hasCitedEntity ?cited .
                ?citation_id schema:creation ?creation .
                ?citation_id schema:timespan ?timespan .
                FILTER(?citation_id = <{citation_uri}>)
            }}
        """
        return self._runSparqlQuery(query)


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
    
        return self._runSparqlQuery(query)
    

    # Returns all citations where the citing and cited entities share at least one author
    def getAllAuthorSelfCitations(self) -> pd.DataFrame: 

        # For every resource ?citation_id that is both a citation and an author self-citation, return:
        # the entity it cites from, the entity it cites, its creation date, and its timespan.
        query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
            PREFIX schema: <https://schema.org/> 

            SELECT ?citation_id ?citing ?cited ?creation ?timespan
            WHERE {
                ?citation_id rdf:type schema:citation .
                ?citation_id rdf:type schema:author_sc .
                ?citation_id schema:hasCitingEntity ?citing .
                ?citation_id schema:hasCitedEntity ?cited .
                ?citation_id schema:creation ?creation .
                ?citation_id schema:timespan ?timespan .
            }
        """
    
        return self._runSparqlQuery(query)
    

    # Returns all citations where the citing and cited entities are published in the same journal
    def getAllJournalSelfCitations(self) -> pd.DataFrame: 

        # For every resource ?citation_id that is both a citation and a journal self-citation, return:
        # the entity it cites from, the entity it cites, its creation date, and its timespan.
        query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
            PREFIX schema: <https://schema.org/> 

            SELECT ?citation_id ?citing ?cited ?creation ?timespan
            WHERE {
                ?citation_id rdf:type schema:citation .
                ?citation_id rdf:type schema:journal_sc .
                ?citation_id schema:hasCitingEntity ?citing .
                ?citation_id schema:hasCitedEntity ?cited .
                ?citation_id schema:creation ?creation .
                ?citation_id schema:timespan ?timespan .
            }
        """
    
        return self._runSparqlQuery(query)
    
    #*** Da rivedere i timespan negativi per capire come gestirli al meglio.
    # Helper method to convert an ISO timespan string (e.g., "P2Y0M16D") into a tuple of integers (years, months, days).
    def _timespan_to_tuple(self, timespan: str) -> tuple: 
        timespan = str(timespan).strip() # Ensure timespan is a string and remove trailing whitespace

        if timespan is None: # Handle None case
            return None
        if not timespan: # Handle empty string case
            return None
        
        y = m = d = 0

        sign = 1 # Handle negative timespans if needed (e.g., "-P2Y" for 2 years in the past)
        if timespan.startswith("-"):
            sign = -1
            timespan = timespan[1:] # Remove the leading "-" for further processing

        if timespan.startswith("P"): # All valid timespans should start with "P"
            timespan = timespan[1:] # Remove the leading "P" for further processing
        else:
            return None # Invalid format, should start with "P"
        
       # Extract years, months, and days from the timespan string 
        if "Y" in timespan:
            y_part, timespan = timespan.split("Y", 1)
            y = int(y_part)
        if "M" in timespan:
            m_part, timespan = timespan.split("M", 1)
            m = int(m_part)
        if "D" in timespan:
            d_part, timespan = timespan.split("D", 1)
            d = int(d_part)

        return (sign * y, sign * m, sign * d) 

    # Returns all citations with timespan within the specified range. 
    # The min_timespan and max_timespan parameters should be in ISO format (e.g., "P2Y0M16D"). 
    def getCitationsWithinTimespan(self, min_timespan: str, max_timespan: str) -> pd.DataFrame: 
        df = self.getAllCitations() # Get all citations from the database

        df["_timespan_tuple"] = df["timespan"].apply(self._timespan_to_tuple) # Convert the timespan column to tuples of integers for easier comparison.
        df = df[df["_timespan_tuple"].notna()] # Filter out rows where timespan is None or invalid

        # Filter the DataFrame based on the provided min_timespan and max_timespan.
        if min_timespan is not None: 
            min_tuple = self._timespan_to_tuple(min_timespan)
            df = df[df["_timespan_tuple"] >= min_tuple] 

        if max_timespan is not None:
            max_tuple = self._timespan_to_tuple(max_timespan)
            df = df[df["_timespan_tuple"] <= max_tuple]

        # Drop the temporary column used for filtering before returning the result as a clean DataFrame.
        return df.drop(columns=["_timespan_tuple"]).reset_index(drop=True) 


    # Returns all citations whose creation date falls within the specified range.
    def getCitationsWithinDate(self, start_date: str, end_date: str) -> pd.DataFrame:
        filters = "" # Build the SPARQL FILTER clause based on the provided parameters

        #* Notes on SPARQL syntax:
        # f-strings are used to dynamically insert the start_date and end_date values into the query.
        # FILTER() filters the results of a SPARQL query based on specified conditions. 
        # STR() converts the RDF value to a string.

        # If the start date is provided only as a year, interpret it as the first day of that year.
        # If it's provided as a year and month, interpret it as the first day of that month.
        if start_date is not None:
            if len(start_date) == 4:
                start_date = start_date + "-01-01"
            elif len(start_date) == 7: 
                start_date = start_date + "-01"
            filters += f'FILTER(?creation_end >= "{start_date}")\n'

        # If the end date is provided only as a year, interpret it as the last day of that year.
        # If it's provided as a year and month, interpret it as the last day of that month (assuming 31 for simplicity).
        if end_date is not None:
            if len(end_date) == 4:
                end_date = end_date + "-12-31"
            elif len(end_date) == 7:
                end_date = end_date + "-31"
            filters += f'FILTER(?creation_start <= "{end_date}")\n'

        #* Notes on SPARQL syntax:
        # BIND() creates new variables based on the existing ones.
        # STRLEN() returns the length of a string.
        # CONCAT() concatenates strings together.
        
        query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
            PREFIX schema: <https://schema.org/> 

            SELECT ?citation_id ?citing ?cited ?creation ?timespan
            WHERE {{
                ?citation_id rdf:type schema:citation .
                ?citation_id schema:hasCitingEntity ?citing .
                ?citation_id schema:hasCitedEntity ?cited .
                ?citation_id schema:creation ?creation .
                ?citation_id schema:timespan ?timespan .

                # Convert the creation value to a string for comparison and normalization.
                BIND(STR(?creation) AS ?creation_str)

                # If the creation value contains only a year, treat it as starting on January 1st.
                # If it contains a year and month, treat it as starting on the first day of that month.
                BIND(
                    IF(STRLEN(?creation_str) = 4,
                        CONCAT(?creation_str, "-01-01"),
                        IF(STRLEN(?creation_str) = 7,
                            CONCAT(?creation_str, "-01"),
                            ?creation_str
                        ))
                    AS ?creation_start
                )

                # If the creation value contains only a year, treat it as ending on December 31st.
                # If it contains a year and month, treat it as ending on the last day of that month 
                # (assuming 31 for simplicity).
                BIND(
                    IF(STRLEN(?creation_str) = 4,
                        CONCAT(?creation_str, "-12-31"),
                        IF(STRLEN(?creation_str) = 7,
                            CONCAT(?creation_str, "-31"),
                            ?creation_str
                        ))
                    AS ?creation_end
                )

                {filters}
            }}
        """
    
        return self._runSparqlQuery(query)

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
        
    def getAuthorsByInternalId(self, internal_id: str) -> pd.DataFrame:
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            query = "SELECT author FROM BibliographicEntity_Authors WHERE internal_id = ?"
            return pd.read_sql(query, con, params=(internal_id,))
        
        #First Helper method

    def getIdsByInternalId(self, internal_id: str) -> pd.DataFrame:
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            query = "SELECT id FROM BibliographicEntity_ID WHERE internal_id = ?"
            return pd.read_sql(query, con, params=(internal_id,))
        
        #Second Helper method 
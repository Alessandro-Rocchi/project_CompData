from .queryHandler import BibliographicEntityQueryHandler, CitationQueryHandler
from .entityClasses import *

class BasicQueryEngine:
    def __init__(self):
        self.citationQuery = [] #list[CitationQueryHandler]. It will store all the objects in the graph database.
        self.bibliographicEntityQuery = [] #list[BibliographicEntityQueryHandler]. It will store all the objects in the relational database.

    def addCitationHandler(self, handler: CitationQueryHandler) -> bool: # handler: we expect an object of that specific class.
        self.citationQuery.append(handler)
        return True #Confirmation of the method .append
    
    def addBibliographicEntityHandler(self, handler: BibliographicEntityQueryHandler) -> bool:
        self.bibliographicEntityQuery.append(handler)
        return True
    
    def cleanCitationHandlers(self) -> bool: #This is used when you want to reset the engine or switch to entirely different database files.
        self.citationQuery = []
        return True
    
    def cleanBibliographicEntityHandlers(self) -> bool:
        self.bibliographicEntityQuery = []
        return True
    
    def getEntityById(self, id: str):
        """
        Queries all handlers to find an entity by its ID.
        Returns the object (BibliographicEntity or Citation) or None
        """
        for handler in self.bibliographicEntityQuery: # you should loop through any database.
            df = handler.getById(id) # You are asking if in the relational database there is this id in a SQL table.
            if df is not None and not df.empty:
                return self._row_to_bibliographic_obj(df.iloc[0]) 
            # ".iloc[]" is Pandas-specific indexer. It concerns the position. 
            # It strips the table structure away from a row to convert it into a clean Python object.
        
        # Search in Citation Handlers (assuming an equivalent search exists)
        for handler in self.citationQuery:
            df = handler.getById(id)
            if df is not None and not df.empty:
                return self._row_to_citation_obj(df.iloc[0])
        return None 
        # if nothing has been found, tell the user that the id doesn't exist.
    
    def _row_to_bibliographic_obj(self, row, bib_entity_class=BibliographicEntity) -> BibliographicEntity:
        bibliographic_entity = bib_entity_class()
       
        bibliographic_entity.title = row.get("title", "")
        bibliographic_entity.publicationDate = row.get("pub_date", "")
        bibliographic_entity.venue = row.get("venue", "")
        bibliographic_entity.authors = row.get("authors", "").split(",") if row.get("authors") else [] # Due to the fact that authors are in another table and they can be more than one, the result must be a list.
        bibliographic_entity.ids = row.get("ids", "").split(",") if row.get("ids") else [] #Same thing as for the authors.

        return bibliographic_entity

    def _row_to_citation_obj(self, row, citation_class=Citation) -> Citation: 
        citation = citation_class()

        citation.ids = [row.get("citation_id", "")]
        citation.creation = row.get("creation", "")
        citation.timespan = row.get("timespan", "")

        # For the citing and cited entities, new BibliographicEntity objects are created 
        # and their IDs are set based on the row data.
        citing_entity = BibliographicEntity()
        citing_entity.ids = [row.get("citing", "")]

        cited_entity = BibliographicEntity()
        cited_entity.ids = [row.get("cited", "")]

        citation.citingEntity = citing_entity
        citation.citedEntity = cited_entity

        return citation
    

    # Returns a list of Citation objects containing all citations retrieved from all citation query handlers.
    def getAllCitations(self) -> list[Citation]:
        all_results = [] # List to store all Citation objects retrieved from all handlers.

        # Iterate over all citation query handlers connected to this engine.
        for handler in self.citationQuery:
            df = handler.getAllCitations() # Ask current handler for all citations as a DataFrame.

            # Convert each DataFrame row into a Citation object.
            for index, row in df.iterrows(): 
                citation = self._row_to_citation_obj(row, Citation)
                all_results.append(citation)

        return all_results 


    # Returns a list of AuthorSelfCitation objects containing all author self-citations retrieved from all citation query handlers.
    def getAllAuthorSelfCitations(self) -> list[AuthorSelfCitation]:
        all_results = [] # List to store all AuthorSelfCitation objects retrieved from all handlers.

        # Iterate over all citation query handlers connected to this engine.
        for handler in self.citationQuery:
            df = handler.getAllAuthorSelfCitations() # Ask current handler for all author self-citations as a DataFrame.

            # Convert each DataFrame row into an AuthorSelfCitation object.
            for index, row in df.iterrows():
                citation = self._row_to_citation_obj(row, AuthorSelfCitation)
                all_results.append(citation)

        return all_results


    # Returns a list of JournalSelfCitation objects containing all journal self-citations retrieved from all citation query handlers.
    def getAllJournalSelfCitations(self) -> list[JournalSelfCitation]:
        all_results = [] # List to store all JournalSelfCitation objects retrieved from all handlers.

        # Iterate over all citation query handlers connected to this engine.
        for handler in self.citationQuery:
            df = handler.getAllJournalSelfCitations() # Ask the current handler for all journal self-citations as a DataFrame.

            # Convert each DataFrame row into a JournalSelfCitation object.
            for index, row in df.iterrows():
                citation = self._row_to_citation_obj(row, JournalSelfCitation)
                all_results.append(citation)

        return all_results


    # Returns a list of Citation objects whose timespan falls within the specified range.
    def getCitationsWithinTimespan(self, min_timespan: str = None, max_timespan: str = None) -> list[Citation]:
        all_results = [] # List to store all Citation objects retrieved from all handlers that fall within the specified timespan range.

        # Iterate over all citation query handlers connected to this engine.
        for handler in self.citationQuery:
            df = handler.getCitationsWithinTimespan(min_timespan, max_timespan) # Ask the current handler for citations within the requested timespan range.

            # Convert each DataFrame row into a Citation object.
            for index, row in df.iterrows():
                citation = self._row_to_citation_obj(row, Citation)
                all_results.append(citation)

        return all_results


    # Returns a list of Citation objects whose creation date falls within the specified range.
    def getCitationsWithinDate(self, start_date: str = None, end_date: str = None) -> list[Citation]:
        all_results = [] # List to store all Citation objects retrieved from all handlers that fall within the specified date range.

        # Iterate over all citation query handlers connected to this engine.
        for handler in self.citationQuery:
            df = handler.getCitationsWithinDate(start_date, end_date) # Ask the current handler for citations within the requested date range.

            # Convert each DataFrame row into a Citation object.
            for index, row in df.iterrows():
                citation = self._row_to_citation_obj(row, Citation)
                all_results.append(citation)

        return all_results
   

    def getAllBibliographicEntities(self) -> list:
        all_results = [] 
        for handler in self.bibliographicEntityQuery:
            df = handler.getAllBibliographicEntities()
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results
    
    def getBibliographicEntitiesWithTitle(self, title: str) -> list:
        all_results = [] 
        for handler in self.bibliographicEntityQuery:
            df = handler.getBibliographicEntitiesWithTitle(title)
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results
    
    def getBibliographicEntitiesWithAuthor(self, author: str) -> list:
        all_results = [] 
        for handler in self.bibliographicEntityQuery:
            df = handler.getBibliographicEntitiesWithAuthor(author)
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results
    
    def getBibliographicEntitiesWithinDate(self, start_date: str = None, end_date: str = None) -> list:
        all_results = []
        for handler in self.bibliographicEntityQuery:
            df = handler.getBibliographicEntitiesWithinPublicationDate(start_date, end_date)
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results
    
    def getBibliographicEntitiesWithVenue(self, venue: str) -> list:
        all_results = [] 
        for handler in self.bibliographicEntityQuery:
            df = handler.getBibliographicEntitiesWithVenue(venue)
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results
    
    
    # def row_to_bibliographic_obj(self, row, handler) -> BibliographicEntity:
    #     # helper that creates specific Bibliographic objects and fetch related IDs/authors
    #     internal_id = row["internal_id"]
    #     entity_type = str(row.get('type', '')).lower()

    #     # Map the correct type (Journal, Book, etc.)
    #     if "journal" in entity_type:
    #         entity = JournalArticle()
    #     elif "book" in entity_type:
    #         entity = Book()
    #     else:
    #         entity = BibliographicEntity()

    #     entity.title = row.get("title", "")
    #     entity.publicationDate = row.get("pub_date", "")
    #     entity.venue = row.get("venue", "")

    #     # Fetch external IDs and authors from the handler that found this record
    #     entity.ids = getIdsByInternalId(internal_id)["id"].tolist()
    #     entity.authors = handler.getAuthorsByInternalId(internal_id)["author"].tolist()

    #     return entity
    
    # def row_to_citation_obj(self, row) -> Citation:
    #     cit_type = str(row.get('type', '')).lower()

    #     if "journal" in cit_type:
    #         cit = JournalSelfCitation()
    #     elif "author" in cit_type:
    #         cit = AuthorSelfCitation()
    #     else:
    #         cit = Citation()
        
    #     # Mapping IDs and attributes (ADJUST KEYS BASED ON OUR DB SCHEMA)
    #     cit.ids = [row.get("citation_id", "")]
    #     cit.creation = row.get('creation', "")
    #     cit.timespan = row.get('timespan', "")
    #     return cit



class FullQueryEngine(BasicQueryEngine):
    def __init__(self):
        super().__init__()
    
    #* Method which takes in input an author name and returns a list of AuthorSelfCitation objects where the given author is both the citing and cited entity.
    def getAuthorSelfCitationsByName(self, author_name: str) -> list[AuthorSelfCitation]:
        result = []
        citation_list = self.getAllAuthorSelfCitations()
        for entity in citation_list:
            if (author_name in entity.getCitingEntity().getAuthors()) and (author_name in entity.getCitedEntity().getAuthors()):
                result.append(entity)
        return result
    
    #* Method which takes in input an author name and returns a list of JournalSelfCitation objects where the given journal is both the citing and cited entity.
    def getJournalSelfCitationsByName(self, journal_name: str) -> list[JournalSelfCitation]:
        result = []
        citation_list = self.getAllJournalSelfCitations()
        for entity in citation_list:
            if (journal_name == entity.getCitingEntity().getVenue()) and (journal_name == entity.getCitedEntity().getVenue()):
                result.append(entity)
        return result
    
    #* Method which takes in input a bibliographic entity title and a date range and returns a list of Citation objects where the given journal is both the citing and cited entity.
    def getCitationsOfBibEntityByTitleWithinDate(self, bib_entity_title: str, min_date: str, max_date: str) -> List[Citation]:
        result = []
        citation_list = self.getCitationsWithinDate(min_date, max_date)
        for entity in citation_list:
            if bib_entity_title in entity.getCitedEntity().getTitle():
                result.append(entity)
        return result
    
    def getReferencesOfBibEntityByTitleWithinTimespan(self, bib_entity_title: str, min_timespan: str, max_timespan: str) -> list[Citation]: #* Method which takes in input a bibliographic entity title and a timespan range and returns a list of Citation objects where the given journal is both the citing and cited entity.
        result = []
        citation_list = self.getCitationsWithinTimespan(min_timespan, max_timespan)
        for entity in citation_list:
            if bib_entity_title in entity.getCitingEntity().getTitle():
                result.append(entity)
        return result
from .queryHandler import BibliographicEntityQueryHandler
from .queryHandler import CitationQueryHandler
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
            if df is not None and not df.empty: # if something is found, return it
                return self.getAllBibliographicEntities()
            if not df.empty: # if something is found, return it
                return self._row_to_bibliographic_obj(df.iloc[0]) # ".iloc[]" is Pandas-specific indexer. It concerns the position. It strips the table structure away from a row to convert it into a clean Python object.
        
        # Search in Citation Handlers (assuming an equivalent search exists)
        for handler in self.citationQuery:
            df = handler.getById(id)
            if df is not None and not df.empty:
                return self.getAllCitations() # Still to be implemented  
            if not df.empty:
                return self._row_to_citation_obj(df.iloc[0])
        return None # if nothing has been found, tell the user that the id doesn't exist.
    
    def _row_to_bibliographic_obj(self, row):
        # Determine the specific class type
        entity_type = row.get('type', '').lower()

        # Map the correct type (Journal, Book, etc.)
        if entity_type == "journal-article":
            b_entity = JournalArticle(ids=[row.get("internal_id", "")])
        elif entity_type == "book":
            b_entity = Book(ids=[row.get("internal_id", "")])
        else:
            b_entity = BibliographicEntity(ids=[row.get("internal_id", "")])

        b_entity.title = row.get("title", "")
        b_entity.publicationDate = row.get("pub_date", "")
        b_entity.venue = row.get("venue", "")

        return b_entity

    # Helper method to convert a DataFrame row into a Citation object.
    def _row_to_citation_obj(self, row, citation_class=Citation) -> Citation: 
        
        #* Previous version
        # ids are not stored in the same way in the relational and graph database
        # Dataframe doesn't have a 'type' column
        """ cit_type = row.get('type', '')
        if cit_type == 'journal-self':
            cit = JournalSelfCitation(ids=[row.get('citation_id', '')])
        elif cit_type == 'author-self':
            cit = AuthorSelfCitation(ids=[row.get('citation_id', '')])
        else:
            cit = Citation(ids=[row.get('citation_id', '')])
        
        cit.creation = row.get('creation', "")
        cit.timespan = row.get('timespan', "")
        return cit """ 

        #* Proposed version with the optional parameter to specify the citation class type. 
        # citation_class is an optional parameter that allows you to specify the type of Citation. 
        # By default, it will create a generic Citation object.
        citation = citation_class()

        citation.ids = [row.get("citation_id", "")]
        citation.creation = row.get("creation", "")
        citation.timespan = row.get("timespan", "")
        citation.citingEntityId = row.get("citing", "")
        citation.citedEntityId = row.get("cited", "")

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
                citation = self._row_to_citation_obj(row, Aut)
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
<<<<<<< HEAD
        #getAllBibliographicEntities method 11
=======

    #getAllBibliographicEntities method 11
>>>>>>> 1feaabb902ffc71440b3ed2f90263bf7d26d6128
    def getAllBibliographicEntities(self) -> list:
        all_results = [] # 1. Final List
        
        # 2. Iteration on each handler of the list
        for handler in self.bibliographicEntityQuery:
            
            # 3. Call the method to obtain the dataframe 
            df = handler.getAllBibliographicEntities()
            
            # 4. Iteration on the rows of the dataframe
            for index, row in df.iterrows():
                internal_id = row["internal_id"]
                
                # 5. Helper methods to obtain the missing data
                df_authors = handler.getAuthorsByInternalId(internal_id)
                df_ids = handler.getIdsByInternalId(internal_id)
                
                # 6. Creation of the object BibliographicEntity
                entity = BibliographicEntity()
                
                # 7. Filling the object with collected data
                entity.title = row["title"]
                entity.publicationDate = row["pub_date"]
                entity.venue = row["venue"]
                # Convert helper DataFrame columns to Python lists
                entity.authors = df_authors["author"].tolist()
                entity.ids = df_ids["id"].tolist()
                
                # 8. add the object to the final list
                all_results.append(entity)
                
        # 9. Return final list 
        return all_results
    
    #getBibliographicEntitiesWithTitle method 12
    def getBibliographicEntitiesWithTitle(self, title: str) -> list:
        all_results = [] 
        
        for handler in self.bibliographicEntityQuery:
            
            df = handler.getBibliographicEntitiesWithTitle(title)
            
            for index, row in df.iterrows():
                internal_id = row["internal_id"]
                
                df_authors = handler.getAuthorsByInternalId(internal_id)
                df_ids = handler.getIdsByInternalId(internal_id)
            
                entity = BibliographicEntity()
                
                entity.title = row["title"]
                entity.publicationDate = row["pub_date"]
                entity.venue = row["venue"]
                entity.authors = df_authors["author"].tolist()
                entity.ids = df_ids["id"].tolist()
                
                all_results.append(entity)
                
        return all_results
    
    #getBibliographicEntitiesWithAuthor method 13
    def getBibliographicEntitiesWithAuthor(self, author: str) -> list:
        all_results = [] 
        
        for handler in self.bibliographicEntityQuery:
            
            df = handler.getBibliographicEntitiesWithAuthor(author)
            
            for index, row in df.iterrows():
                internal_id = row["internal_id"]
                
                df_authors = handler.getAuthorsByInternalId(internal_id)
                df_ids = handler.getIdsByInternalId(internal_id)
                
                entity = BibliographicEntity()
                
                entity.title = row["title"]
                entity.publicationDate = row["pub_date"]
                entity.venue = row["venue"]
                entity.authors = df_authors["author"].tolist()
                entity.ids = df_ids["id"].tolist()
                
                all_results.append(entity)
                
        return all_results
    
    #getBibliographicEntitiesWithinDate method 14
    def getBibliographicEntitiesWithinDate(self, start_date: str = None, end_date: str = None) -> list:
        all_results = []

        for handler in self.bibliographicEntityQuery:
            
            df = handler.getBibliographicEntitiesWithinPublicationDate(start_date, end_date)
        
            for index, row in df.iterrows():
               internal_id = row["internal_id"]
               df_authors = handler.getAuthorsByInternalId(internal_id)
               df_ids = handler.getIdsByInternalId(internal_id)
            
               entity = BibliographicEntity()
               entity.title = row["title"]
               entity.publicationDate = row["pub_date"]
               entity.venue = row["venue"]
               entity.authors = df_authors["author"].tolist()
               entity.ids = df_ids["id"].tolist()

               all_results.append(entity)
            
        return all_results
    
    #getBibliographicEntitiesWithVenue method 15
    def getBibliographicEntitiesWithVenue(self, venue: str) -> list:
        all_results = [] 
        
        for handler in self.bibliographicEntityQuery:
            
            df = handler.getBibliographicEntitiesWithVenue(venue)
            
            for index, row in df.iterrows():
                internal_id = row["internal_id"]
                
                df_authors = handler.getAuthorsByInternalId(internal_id)
                df_ids = handler.getIdsByInternalId(internal_id)
                
                entity = BibliographicEntity()
                
                entity.title = row["title"]
                entity.publicationDate = row["pub_date"]
                entity.venue = row["venue"]
                entity.authors = df_authors["author"].tolist()
                entity.ids = df_ids["id"].tolist()
                
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
    
    def getAuthorSelfCitationByName(self, author_name: str) -> list[AuthorSelfCitation]: #* Method which takes in input an author name and returns a list of AuthorSelfCitation objects where the given author is both the citing and cited entity.
        result = []
        citation_list = self.getAllAuthorSelfCitations()
        for entity in citation_list:
            if (author_name in entity.getCitingEntity().getAuthors()) and (author_name in entity.getCitedEntity().getAuthors()):
                result.append(entity)
        return result
    
    def getJournalSelfCitationByName(self, journal_name: str) -> list[JournalSelfCitation]: #* Method which takes in input an author name and returns a list of JournalSelfCitation objects where the given journal is both the citing and cited entity.
        result = []
        citation_list = self.getAllJournalSelfCitations()
        for entity in citation_list:
            if (journal_name == entity.getCitingEntity().getVenue()) and (journal_name == entity.getCitedEntity().getVenue()):
                result.append(entity)
        return result
    
    def getCitationsOfBibEntityByTitleWithinDate(self, bib_entity_title: str, min_date: str, max_date: str) -> list[Citation]: #* Method which takes in input a bibliographic entity title and a date range and returns a list of Citation objects where the given journal is both the citing and cited entity.
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
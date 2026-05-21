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
        for handler in self.bibliographicEntityQuery: # It loops through any database.
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
    
    def _row_to_bibliographic_obj(self, row) -> BibliographicEntity:
        entity = BibliographicEntity()
        entity.title = row.get("title", "")
        entity.publicationDate = row.get("pub_date", "")
        entity.venue = row.get("venue", "")
        
        authors_raw = str(row.get("authors", ""))
        row_ids = str(row.get('ids', ""))

        clean_author_list = []
        clean_ids_list = []

        if authors_raw and authors_raw != "None":
            splitted_authors = authors_raw.split(';')
            
            author_stripped = [author.strip() for author in splitted_authors if author.strip()]
            
            clean_author_list = list(set(author_stripped))

            if row_ids and row_ids != "None":
                ids_splitted = row_ids.split(';')
                ids_stripped = [id_str.strip() for id_str in ids_splitted if id_str.strip()]
                clean_ids_list = list(set(ids_stripped))

        entity.authors = clean_author_list
        entity.ids = clean_ids_list
        return entity

    # Helper method to convert a DataFrame row into a Citation object. 
    # It takes in input the row and the class of citation to create.
    def _row_to_citation_obj(self, row, citation_class=Citation) -> Citation: 
        citation = citation_class() 

        citation.ids = [row.get("citation_id", "")] 
        citation.creation = row.get("creation", "")
        citation.timespan = row.get("timespan", "")
        
        citing_entity = BibliographicEntity() 
        citing_entity.ids = [row.get("citing", "")]

        cited_entity = BibliographicEntity()
        cited_entity.ids = [row.get("cited", "")]

        citation.hasCitingEntity = citing_entity 
        citation.hasCitedEntity = cited_entity

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
   

    def getAllBibliographicEntities(self) -> list[BibliographicEntity]:
        all_results = [] 
        for handler in self.bibliographicEntityQuery:
            df = handler.getAllBibliographicEntities()
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results
    
    def getBibliographicEntitiesWithTitle(self, title: str) -> list[BibliographicEntity]:
        all_results = [] 
        for handler in self.bibliographicEntityQuery:
            df = handler.getBibliographicEntitiesWithTitle(title)
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results
    
    def getBibliographicEntitiesWithAuthor(self, author: str) -> list[BibliographicEntity]:
        all_results = [] 
        for handler in self.bibliographicEntityQuery:
            df = handler.getBibliographicEntitiesWithAuthor(author)
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results
    
    def getBibliographicEntitiesWithinDate(self, start_date: str = None, end_date: str = None) -> list[BibliographicEntity]:
        all_results = []
        for handler in self.bibliographicEntityQuery:
            df = handler.getBibliographicEntitiesWithinPublicationDate(start_date, end_date)
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results
    
    def getBibliographicEntitiesWithVenue(self, venue: str) -> list[BibliographicEntity]:
        all_results = [] 
        for handler in self.bibliographicEntityQuery:
            df = handler.getBibliographicEntitiesWithVenue(venue)
            for index, row in df.iterrows():
                entity = self._row_to_bibliographic_obj(row)
                all_results.append(entity)
        return all_results


class FullQueryEngine(BasicQueryEngine):
    def __init__(self):
        super().__init__()
    
    #* Method which takes in input an author name and returns a list of AuthorSelfCitation objects where the given author is both the citing and cited entity.
    def getAuthorSelfCitationsByName(self, author_name: str) -> list[AuthorSelfCitation]:
        result = []
        valid_bib_entity = dict()
        all_author_citations = self.getAllAuthorSelfCitations()
        bib_entity_with_authors = self.getBibliographicEntitiesWithAuthor(author_name)
        for bib in bib_entity_with_authors:
            print(bib.getAuthors())
            valid_bib_entity[bib.getIds()[0]] = bib
        
        if not valid_bib_entity:
            return []
        
        for citation in all_author_citations:
            if (citation.getCitedEntity().getIds()[0] in valid_bib_entity) and (citation.getCitingEntity().getIds()[0] in valid_bib_entity):
                full_cited = valid_bib_entity[citation.getCitedEntity().getIds()[0]]
                full_citing = valid_bib_entity[citation.getCitingEntity().getIds()[0]]

                citing_author = {a for a in full_citing}
                cited_author = {a for a in full_cited}

                author_common = citing_author.intersection(cited_author)
                if any(author_name in author for author in author_common):
                    citation.hasCitedEntity = full_cited
                    citation.hasCitingEntity = full_citing
                    result.append(citation)
        return result
    
    #* Method which takes in input an author name and returns a list of JournalSelfCitation objects where the given journal is both the citing and cited entity.
    def getJournalSelfCitationsByName(self, journal_name: str) -> list[JournalSelfCitation]:
        result = []
        valid_bib_entity = dict()
        all_journal_citations = self.getAllJournalSelfCitations()
        bib_entity_with_venues = self.getBibliographicEntitiesWithVenue(journal_name)
        for bib in bib_entity_with_venues:
            valid_bib_entity[bib.getIds()[0]] = bib
        
        if not valid_bib_entity:
            return []
        
        for citation in all_journal_citations:
            if (citation.getCitedEntity().getIds()[0] in valid_bib_entity) and (citation.getCitingEntity().getIds()[0] in valid_bib_entity):
                full_cited = valid_bib_entity[citation.getCitedEntity().getIds()[0]]
                full_citing = valid_bib_entity[citation.getCitingEntity().getIds()[0]]
                venue_citing = (full_citing.getVenue() or "")
                venue_cited = (full_cited.getVenue() or "")
                if (journal_name in venue_citing) and (journal_name in venue_cited):
                    citation.hasCitedEntity = full_cited
                    citation.hasCitingEntity = full_citing
                    result.append(citation)
        return result
    
    
    #* Method which takes in input a bibliographic entity title and a date range and returns a list of Citation objects where the given journal is both the citing and cited entity.
    def getCitationsOfBibEntityByTitleWithinDate(self, bib_entity_title: str, min_date: str, max_date: str) -> list[Citation]:
        result = []
        tmp_cache = dict()
        valid_bib_entity = dict()
        bib_entity_with_title = self.getBibliographicEntitiesWithTitle(bib_entity_title)
        for bib in bib_entity_with_title:
            pub_date = bib.getPublicationDate() or ""
            if pub_date and (min_date <= pub_date <= max_date):
                valid_bib_entity[bib.getIds()[0]] = bib
        
        if not valid_bib_entity:
            return []
        
        all_citation = self.getAllCitations()
        for citation in all_citation:
            if citation.getCitedEntity().getIds()[0] in valid_bib_entity:
                full_cited = valid_bib_entity[citation.getCitedEntity().getIds()[0]]

                if citation.getCitingEntity().getIds()[0] not in tmp_cache:
                    tmp_cache[citation.getCitingEntity().getIds()[0]] = self.getEntityById(citation.getCitingEntity().getIds()[0])
                full_citing = tmp_cache[citation.getCitingEntity().getIds()[0]]

                citation.hasCitingEntity = full_citing
                citation.hasCitedEntity = full_cited
                result.append(citation)
        return result
    
    def getReferencesOfBibEntityByTitleWithinTimespan(self, bib_entity_title: str, min_timespan: str, max_timespan: str) -> list[Citation]: #* Method which takes in input a bibliographic entity title and a timespan range and returns a list of Citation objects where the given journal is both the citing and cited entity.
        result = []
        tmp_cache = dict()
        valid_bib_entity = dict()
        bib_entity_with_title = self.getBibliographicEntitiesWithTitle(bib_entity_title)

        for bib in bib_entity_with_title:
           valid_bib_entity[bib.getIds()[0]] = bib
        
        if not valid_bib_entity:
            return []
        
        all_citation = self.getCitationsWithinTimespan(min_timespan, max_timespan)
        for citation in all_citation:

            if citation.getCitingEntity().getIds()[0] in valid_bib_entity:
                full_citing = valid_bib_entity[citation.getCitingEntity().getIds()[0]]

                if citation.getCitedEntity().getIds()[0] not in tmp_cache:
                    tmp_cache[citation.getCitedEntity().getIds()[0]] = self.getEntityById(citation.getCitedEntity().getIds()[0])
                full_cited = tmp_cache[citation.getCitedEntity().getIds()[0]]

                citation.hasCitingEntity = full_citing
                citation.hasCitedEntity = full_cited
                result.append(citation)
        return result
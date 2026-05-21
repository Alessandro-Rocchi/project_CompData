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
        valid_bib_entity = dict() #* A dictionary to store valid bibliographic entities that match the given author name. The keys are the IDs of the bibliographic entities, and the values are the corresponding BibliographicEntity objects.
        all_author_citations = self.getAllAuthorSelfCitations() #* A list of all AuthorSelfCitation objects retrieved from the citation query handlers. This list will be filtered to find those that involve the specified author as both the citing and cited entity.
        bib_entity_with_authors = self.getBibliographicEntitiesWithAuthor(author_name) #* A list of BibliographicEntity objects that have the specified author in their authorship. This list is used to identify which bibliographic entities are relevant for the author self-citation analysis.
        
        for bib in bib_entity_with_authors: #* Loop through each bibliographic entity that has the specified author and populate the valid_bib_entity dictionary with their IDs and corresponding objects. This allows for quick lookup of bibliographic entities by their IDs when processing the citations.
            for bib_id in bib.getIds():
                valid_bib_entity[str(bib_id)] = bib
        
        if not valid_bib_entity: #* If there are no valid bibliographic entities found for the specified author, return an empty list since there can be no author self-citations without relevant bibliographic entities.
            return []
        
        for citation in all_author_citations: #* Loop through each AuthorSelfCitation object and check if both the cited and citing entities are in the valid_bib_entity dictionary. If they are, retrieve the full bibliographic entities for both the cited and citing entities.
            if (citation.getCitedEntity().getIds()[0] in valid_bib_entity) and (citation.getCitingEntity().getIds()[0] in valid_bib_entity):
                full_cited = valid_bib_entity[citation.getCitedEntity().getIds()[0]]
                full_citing = valid_bib_entity[citation.getCitingEntity().getIds()[0]]

                citing_author = {a.lower() for a in full_citing.getAuthors()}#* Create a set of lowercase author names for the citing entity to facilitate case-insensitive comparison.
                cited_author = {a.lower() for a in full_cited.getAuthors()}#* Create a set of lowercase author names for the cited entity to facilitate case-insensitive comparison.

                author_common = citing_author.intersection(cited_author)#* Find the common authors between the citing and cited entities. If there is any common author that matches the specified author name (case-insensitive), then this citation is considered an author self-citation for that author.
                
                if any(author_name.lower() in author for author in author_common):#* Check if the specified author name (in lowercase) is present in any of the common authors. If it is, then this citation is relevant for the result.
                    citation.hasCitedEntity = full_cited
                    citation.hasCitingEntity = full_citing
                    result.append(citation)
        return result
    
    #* Method which takes in input an author name and returns a list of JournalSelfCitation objects where the given journal is both the citing and cited entity.
    def getJournalSelfCitationsByName(self, journal_name: str) -> list[JournalSelfCitation]:
        result = []
        valid_bib_entity = dict() #* A dictionary to store valid bibliographic entities that match the given journal name. The keys are the IDs of the bibliographic entities, and the values are the corresponding BibliographicEntity objects.
        all_journal_citations = self.getAllJournalSelfCitations() #* A list of all JournalSelfCitation objects retrieved from the citation query handlers. This list will be filtered to find those that involve the specified journal as both the citing and cited entity.
        bib_entity_with_venues = self.getBibliographicEntitiesWithVenue(journal_name) #* A list of BibliographicEntity objects that have the specified journal name in their venue field. This list is used to identify which bibliographic entities are relevant for the journal self-citation analysis.
        
        for bib in bib_entity_with_venues: #* Loop through each bibliographic entity that has the specified journal name in its venue and populate the valid_bib_entity dictionary with their IDs and corresponding objects. This allows for quick lookup of bibliographic entities by their IDs when processing the citations.
            for bib_id in bib.getIds():
                valid_bib_entity[str(bib_id)] = bib
        
        if not valid_bib_entity:#* If there are no valid bibliographic entities found for the specified journal name, return an empty list since there can be no journal self-citations without relevant bibliographic entities.
            return []
        
        for citation in all_journal_citations:#* Loop through each JournalSelfCitation object and check if both the cited and citing entities are in the valid_bib_entity dictionary. If they are, retrieve the full bibliographic entities for both the cited and citing entities.
            if (citation.getCitedEntity().getIds()[0] in valid_bib_entity) and (citation.getCitingEntity().getIds()[0] in valid_bib_entity):#* Check if both the cited and citing entities of the citation are present in the valid_bib_entity dictionary, which means they are relevant for the journal self-citation analysis based on the specified journal name.
                full_cited = valid_bib_entity[citation.getCitedEntity().getIds()[0]]
                full_citing = valid_bib_entity[citation.getCitingEntity().getIds()[0]]
                
                venue_citing = (full_citing.getVenue() or "").lower()#* Get the venue of the citing entity and convert it to lowercase for case-insensitive comparison. If the venue is None, use an empty string to avoid errors.
                venue_cited = (full_cited.getVenue() or "").lower()#* Get the venue of the cited entity and convert it to lowercase for case-insensitive comparison. If the venue is None, use an empty string to avoid errors.
                
                if (journal_name.lower() in venue_citing) and (journal_name.lower() in venue_cited):#* Check if the specified journal name (in lowercase) is present in the venue of both the citing and cited entities. If it is, then this citation is considered a journal self-citation for that journal.
                    citation.hasCitedEntity = full_cited
                    citation.hasCitingEntity = full_citing
                    result.append(citation)
        return result
    
    
    #* Method which takes in input a bibliographic entity title and a date range and returns a list of Citation objects where the given journal is both the citing and cited entity.
    def getCitationsOfBibEntityByTitleWithinDate(self, bib_entity_title: str, min_date: str, max_date: str) -> list[Citation]:
        result = []
        tmp_cache = dict()#* A temporary cache dictionary to store full bibliographic entities that have been retrieved during the processing of citations. The keys are the IDs of the bibliographic entities, and the values are the corresponding BibliographicEntity objects. This cache is used to avoid redundant calls to getEntityById for the same bibliographic entity when processing multiple citations that involve it.
        bib_ids = set()#* A set to store the IDs of bibliographic entities that match the given title. This set is used to quickly check if a bibliographic entity involved in a citation is relevant based on its title before retrieving the full entity details.
        valid_bib_entity = dict()#* A dictionary to store valid bibliographic entities that match both the given title and fall within the specified date range. The keys are the IDs of the bibliographic entities, and the values are the corresponding BibliographicEntity objects. This dictionary is used to identify which bibliographic entities are relevant for the citation analysis based on both title and publication date criteria.
        bib_entity_with_title = self.getBibliographicEntitiesWithTitle(bib_entity_title)#* A list of BibliographicEntity objects that have the specified title. This list is used to identify which bibliographic entities are relevant for the citation analysis based on the title criterion.
        bib_entity_within_date = self.getBibliographicEntitiesWithinDate(min_date, max_date)#* A list of BibliographicEntity objects that have a publication date within the specified date range. This list is used to identify which bibliographic entities are relevant for the citation analysis based on the publication date criterion.
        
        for bib in bib_entity_with_title:#* Loop through each bibliographic entity that has the specified title and populate the bib_ids set with their IDs. This allows for quick checking of whether a bibliographic entity involved in a citation has the relevant title before further processing.
            for bib_id in bib.getIds():
                bib_ids.add(str(bib_id))
        
        for bib in bib_entity_within_date:#* Loop through each bibliographic entity that has a publication date within the specified date range and check if its IDs intersect with the bib_ids set. If there is an intersection, it means this bibliographic entity matches both the title and date criteria, and it is added to the valid_bib_entity dictionary for further analysis of citations.
            current_bib_ids = {str(i) for i in bib.getIds()}
            
            if current_bib_ids.intersection(bib_ids):#* Check if there is any intersection between the IDs of the current bibliographic entity (which matches the date criterion) and the set of IDs of bibliographic entities that match the title criterion. If there is an intersection, it means this bibliographic entity satisfies both criteria and is relevant for the citation analysis.
                for bib_id in current_bib_ids:
                    valid_bib_entity[bib_id] = bib
        
        if not valid_bib_entity:#* If there are no valid bibliographic entities that match both the title and date criteria, return an empty list since there can be no relevant citations without valid bibliographic entities.
            return []
        
        all_citation = self.getAllCitations()#* A list of all Citation objects retrieved from the citation query handlers. This list will be filtered to find those that involve the valid bibliographic entities as both the citing and cited entities.
        for citation in all_citation:
            if citation.getCitedEntity().getIds()[0] in valid_bib_entity:#* Check if the cited entity of the citation is in the valid_bib_entity dictionary, which means it is relevant based on the title and date criteria. If it is, retrieve the full bibliographic entity for the cited entity from the valid_bib_entity dictionary.
                full_cited = valid_bib_entity[citation.getCitedEntity().getIds()[0]]

                if citation.getCitingEntity().getIds()[0] not in tmp_cache:#* Check if the citing entity of the citation has already been retrieved and stored in the tmp_cache. If it has not been cached, call getEntityById to retrieve the full bibliographic entity for the citing entity and store it in the tmp_cache. This caching mechanism helps to avoid redundant retrievals of the same bibliographic entity when processing multiple citations that involve it.
                    tmp_cache[citation.getCitingEntity().getIds()[0]] = self.getEntityById(citation.getCitingEntity().getIds()[0])
                full_citing = tmp_cache[citation.getCitingEntity().getIds()[0]]

                if full_citing:#* If the full bibliographic entity for the citing entity was successfully retrieved (i.e., it is not None), then this citation is relevant for the result. Set the hasCitingEntity and hasCitedEntity attributes of the citation to the full bibliographic entities and add the citation to the result list.
                    citation.hasCitingEntity = full_citing
                    citation.hasCitedEntity = full_cited
                    result.append(citation)
        return result
    
    def getReferencesOfBibEntityByTitleWithinTimespan(self, bib_entity_title: str, min_timespan: str, max_timespan: str) -> list[Citation]: #* Method which takes in input a bibliographic entity title and a timespan range and returns a list of Citation objects where the given journal is both the citing and cited entity.
        result = []
        tmp_cache = dict()#* A temporary cache dictionary to store full bibliographic entities that have been retrieved during the processing of citations. The keys are the IDs of the bibliographic entities, and the values are the corresponding BibliographicEntity objects. This cache is used to avoid redundant calls to getEntityById for the same bibliographic entity when processing multiple citations that involve it.
        valid_bib_entity = dict()#* A dictionary to store valid bibliographic entities that match the given title and fall within the specified timespan range. The keys are the IDs of the bibliographic entities, and the values are the corresponding BibliographicEntity objects. This dictionary is used to identify which bibliographic entities are relevant for the citation analysis based on both title and timespan criteria.
        bib_entity_with_title = self.getBibliographicEntitiesWithTitle(bib_entity_title)#* A list of BibliographicEntity objects that have the specified title. This list is used to identify which bibliographic entities are relevant for the citation analysis based on the title criterion.

        for bib in bib_entity_with_title:#* Loop through each bibliographic entity that has the specified title and populate the valid_bib_entity dictionary with their IDs and corresponding objects. This allows for quick lookup of bibliographic entities by their IDs when processing the citations to check if they match the title criterion.
            for bib_id in bib.getIds():
                valid_bib_entity[str(bib_id)] = bib
        
        if not valid_bib_entity:#* If there are no valid bibliographic entities that match the specified title, return an empty list since there can be no relevant citations without valid bibliographic entities.
            return []
        
        all_citation = self.getCitationsWithinTimespan(min_timespan, max_timespan)#* A list of Citation objects whose timespan falls within the specified range, retrieved from the citation query handlers. This list will be filtered to find those that involve the valid bibliographic entities as both the citing and cited entities.
        for citation in all_citation:

            if citation.getCitingEntity().getIds()[0] in valid_bib_entity:#* Check if the citing entity of the citation is in the valid_bib_entity dictionary, which means it is relevant based on the title criterion. If it is, retrieve the full bibliographic entity for the citing entity from the valid_bib_entity dictionary.
                full_citing = valid_bib_entity[citation.getCitingEntity().getIds()[0]]

                if citation.getCitedEntity().getIds()[0] not in tmp_cache:#* Check if the cited entity of the citation has already been retrieved and stored in the tmp_cache. If it has not been cached, call getEntityById to retrieve
                    tmp_cache[citation.getCitedEntity().getIds()[0]] = self.getEntityById(citation.getCitedEntity().getIds()[0])
                full_cited = tmp_cache[citation.getCitedEntity().getIds()[0]]
                
                if full_cited:#* If the full bibliographic entity for the cited entity was successfully retrieved (i.e., it is not None), then this citation is relevant for the result. Set the hasCitingEntity and hasCitedEntity attributes of the citation to the full bibliographic entities and add the citation to the result list.
                    citation.hasCitingEntity = full_citing
                    citation.hasCitedEntity = full_cited
                    result.append(citation)
        return result
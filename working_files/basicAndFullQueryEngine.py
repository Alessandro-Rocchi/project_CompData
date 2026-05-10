from queryHandler import BibliographicEntityQueryHandler
from queryHandler import CitationQueryHandler
from entityClasses import BibliographicEntity
from entityClasses import *

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
            if not df.empty: # if something is found, return it
                return self.row_to_bibliographic_obj(df.iloc[0]) # ".iloc[]" is Pandas-specific indexer. It concerns the position. It strips the table structure away from a row to convert it into a clean Python object.
        # Search in Citation Handlers (assuming an equivalent search exists)
        # Note: Your current CitationQueryHandler doesn't have a getById, 
        # but the BasicQueryEngine is designed to check all sources.
        for handler in self.citationQuery:
            df = handler.getById(id)
            if not df.empty:
                return self.row_to_citation_obj(df.iloc[0])

        return None # if nothing has been found, tell the user that the id doesn't exist.
    
    def row_to_bibliographic_obj(self, row):
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
    
    def row_to_citation_obj(self, row):
        cit_type = row.get('type', '')
        if cit_type == 'journal-self':
            cit = JournalSelfCitation(ids=[row.get('citation_id', '')])
        elif cit_type == 'author-self':
            cit = AuthorSelfCitation(ids=[row.get('citation_id', '')])
        else:
            cit = Citation(ids=[row.get('citation_id', '')])
        
        cit.creation = row.get('creation', "")
        cit.timespan = row.get('timespan', "")
        return cit
    
    #getAllBibliographicEntities method 11
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

class FullQueryEngine(BasicQueryEngine):
    def __init__(self):
        super().__init__()
    
    def getAuthorSelfCitationByName(self, author_name: str) -> list[AuthorSelfCitation]:
        result = []
        citation_list = self.getAllAuthorSelfCitation()
        for entity in citation_list:
            if (author_name in entity.getCitingEntity().getAuthors()) and (author_name in entity.getCitedEntity().getAuthors()):
                result.append(entity)
        return result
    
    def getJournalSelfCitationByName(self, journal_name: str) -> list[JournalSelfCitation]:
        result = []
        citation_list = self.getAllJournalSelfCitation()
        for entity in citation_list:
            if (journal_name == entity.getCitingEntity().getVenue()) and (journal_name == entity.getCitedEntity().getVenue()):
                result.append(entity)
        return result
    
    def getCitationsOfBibEntityByTitleWithinDate(self, bib_entity_title: str, min_date: str, max_date: str) -> list[Citation]:
        result = []
        citation_list = self.getCitationscEntitiesWithinDate(min_date, max_date)
        for entity in citation_list:
            if bib_entity_title in entity.getCitedEntity().getTitle():
                result.append(entity)
        return result
    
    def getReferencesOfBibEntityByTitleWithinTimespan(self, bib_entity_title: str, min_timespan: str, max_timespan: str) -> list[Citation]:
        result = []
        citation_list = self.getCitationscEntitiesWithinTimespan(min_timespan, max_timespan)
        for entity in citation_list:
            if bib_entity_title in entity.getCitingEntity().getTitle():
                result.append(entity)
        return result


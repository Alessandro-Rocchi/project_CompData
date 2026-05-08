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
    
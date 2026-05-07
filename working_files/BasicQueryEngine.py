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
    
    def cleanBibliographicEntityQuery(self) -> bool:
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
                return self.row_to_bibliographic_obj(df.iloc[0])
        # Search in Citation Handlers (assuming an equivalent search exists)
        # Note: Your current CitationQueryHandler doesn't have a getById, 
        # but the BasicQueryEngine is designed to check all sources.
        for handler in self.citationQuery:
            if hasattr(handler, 'getById'):
                df = handler.getById(id)
                if not df.empty:
                    return self.row_to_citation_obj(df.iloc[0])

        return None # if nothing has been found, tell the user that the id doesn't exist.
    
    def row_to_bibliographic_obj(self, row):
        # We use .get() to avoid KeyErrors if a column is missing
        # We pass the internal_id or the primary ID to the constructor
        b_entity = BibliographicEntity(ids=[row.get('internal_id', '')])
        b_entity.title = row.get('title', "")
        b_entity.publicationDate = row.get('pub_date', "")
        b_entity.venue = row.get('venue', "")
        # Note: Authors and extra IDs would require a second query 
        # using the helper methods Member 1 wrote.
        return b_entity
    
    def row_to_citation_obj(self, row, citation_type=Citation):
        cit = citation_type(ids=[row.get('citation_id', '')])
        cit.creation = row.get('creation', "")
        cit.timespan = row.get('timespan', "")
        return cit
    
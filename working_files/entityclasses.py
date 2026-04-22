class IdentifiableEntity:
    def __init__(self):
        self.ids = []

    def getIds(self):
        return self.ids

class BibliographicEntity(IdentifiableEntity):
    def __init__(self, ids):
        
        super().__init__(ids)
        
        self.title = ""
        self.authors = []
        self.publicationDate = ""
        self.venue = ""

    def getTitle(self):
        return self.title

    def getAuthors(self):
        return self.authors

    def getPublicationDate(self):
        return self.publicationDate

    def getVenue(self):
        return self.venue
    

class Citation(IdentifiableEntity):
    def __init__(self, ids):
        super().__init__(ids)
        self.creation = ""
        self.timespan = ""

    def getCreation(self) -> str:
        return self.creation
    
    def getTimepasn(self) -> str:
        return self.timespan
    
    def getCitingEntity(self) -> BibliographicEntity:
        citingEntity = BibliographicEntity()
        return citingEntity
    
    def getCitingEntity(self) -> BibliographicEntity:
        citedEntity = BibliographicEntity()
        return citedEntity

class JournalSelfCitation(Citation):
    def __init__(self, ids):
        super().__init__(ids)

class AuthorSelfCitation(Citation):
    def __init__(self, ids):
        super().__init__(ids)
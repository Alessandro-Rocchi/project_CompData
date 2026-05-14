class IdentifiableEntity:
    def __init__(self):
        self.ids = []

    def getIds(self):
        return self.ids

class BibliographicEntity(IdentifiableEntity):
    def __init__(self):
        super().__init__()
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
    def __init__(self):
        super().__init__()
        self.creation = ""
        self.timespan = ""
        self.citingEntity = None # Ho aggiunto queste due
        self.citedEntity = None

    def getCreation(self) -> str:
        return self.creation
    
<<<<<<< HEAD
    def getTimespan(self) -> str: # Spelling
=======
    def getTimespan(self) -> str:
>>>>>>> 1d02f119ac64c803e03c542a64f6d633a5a1b0f5
        return self.timespan
    
    def getCitingEntity(self) -> BibliographicEntity:
        citingEntity = BibliographicEntity()
        return citingEntity
    
<<<<<<< HEAD
    def getCitedEntity(self) -> BibliographicEntity: # C'era scritto due volte "Citing"
=======
    def getCitedEntity(self) -> BibliographicEntity:
>>>>>>> 1d02f119ac64c803e03c542a64f6d633a5a1b0f5
        citedEntity = BibliographicEntity()
        return citedEntity

class JournalSelfCitation(Citation):
    def __init__(self):
        super().__init__()

class AuthorSelfCitation(Citation):
    def __init__(self):
        super().__init__()
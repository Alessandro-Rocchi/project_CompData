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
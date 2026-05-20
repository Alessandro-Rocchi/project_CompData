# This is a base class for entities that can be identified by an ID.
# It has a list of IDs and a method to get the IDs.
class IdentifiableEntity: 
    def __init__(self):
        self.ids = []

    def getIds(self)-> list[str]:
        return self.ids
    
# This class represents a bibliographic entity, such as a paper or a book. 
# It inherits from IdentifiableEntity and has additional attributes and getter methods 
# for title, authors, publication date, and venue.
class BibliographicEntity(IdentifiableEntity): 
    def __init__(self):
        super().__init__()
        self.title = ""
        self.authors = []
        self.publicationDate = ""
        self.venue = ""

    def getTitle(self) -> str:
        return self.title

    def getAuthors(self) -> list[str]:
        return self.authors

    def getPublicationDate(self) -> str:
        return self.publicationDate

    def getVenue(self) -> str:
        return self.venue
    
# This class represents a citation, which is a relationship between two bibliographic entities. 
# It inherits from IdentifiableEntity and has additional attributes and getter methods
# for creation date, timespan, citing entity, and cited entity. 
class Citation(IdentifiableEntity): 
    def __init__(self):
        super().__init__()
        self.creation = ""
        self.timespan = ""
        self.citingEntity = None
        self.citedEntity = None

    def getCreation(self) -> str:
        return self.creation
    
    def getTimespan(self) -> str:
        return self.timespan
    
    def getCitingEntity(self) -> BibliographicEntity:
        return self.citingEntity
    
    def getCitedEntity(self) -> BibliographicEntity:
        return self.citedEntity

# This class represents a journal self-citation, which is a specific type of citation 
# where the citing and cited entities are the same journal. It inherits from Citation 
# and does not have any additional attributes or methods, but it can be used to differentiate 
# journal self-citations from other types of citations in the RDF graph.
class JournalSelfCitation(Citation): 
    def __init__(self):
        super().__init__()

# This class represents an author self-citation, which is a specific type of citation 
# where the citing and cited entities share at least one author. It inherits from Citation 
# and does not have any additional attributes or methods, but it can be used to differentiate 
# author self-citations from other types of citations in the RDF graph.
class AuthorSelfCitation(Citation): 
    def __init__(self):
        super().__init__()

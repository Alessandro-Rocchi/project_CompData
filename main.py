from working_files.uploadCitationAndBibliographic import CitationUploadHandler, BibliographicEntityUploadHandler
from working_files.queryHandler import BibliographicEntityQueryHandler, CitationQueryHandler
from working_files.basicAndFullQueryEngine import BasicQueryEngine
from test import TestProjectBasic


def main():
    t = TestProjectBasic()
    print(t.test_01_CitationUploadHandler())
    print(t.test_02_BibliographicEntityUploadHandler())
    print(t.test_03_CitationQueryHandler())
    print(t.test_04_ProcessDataQueryHandler())
    print(t.test_05_FullQueryEngine())
    
if __name__ == "__main__":
    main()
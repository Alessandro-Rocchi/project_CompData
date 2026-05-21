from test import TestProjectBasic
from working_files.basicAndFullQueryEngine import FullQueryEngine
from working_files.queryHandler import CitationQueryHandler, BibliographicEntityQueryHandler
from working_files.uploadCitationAndBibliographic import CitationUploadHandler, BibliographicEntityUploadHandler

def main():

    rel_path = "relational.db"
    bib = BibliographicEntityUploadHandler()
    bib.setDbPathOrUrl(rel_path)
    bib.pushDataToDb("data/dh_metadata.json")
    grp_endpoint = "http://192.168.1.50:9999/blazegraph/sparql"
    jou = CitationUploadHandler()
    jou.setDbPathOrUrl(grp_endpoint)
    jou.pushDataToDb("data/dh_citations.csv")
    bib_qh = BibliographicEntityQueryHandler()
    bib_qh.setDbPathOrUrl(rel_path)
    jou_qh = CitationQueryHandler()
    jou_qh.setDbPathOrUrl(grp_endpoint)
    que = FullQueryEngine()
    que.addBibliographicEntityHandler(bib_qh)
    que.addCitationHandler(jou_qh)

    result_q2 = que.getEntityById("0603926665-06180334360")
    print("Result Q2:")
    print(result_q2)
    result_q3 = que.getAuthorSelfCitationsByName("Leymann, Frank")
    print("Result Q3:")
    print(result_q3)
    result_q4 = que.getJournalSelfCitationsByName("Digital Scholarship In The Humanities")
    print("Result Q4:")
    for r in result_q4:
        print(r.getCitingEntity().getTitle(), "cites", r.getCitedEntity().getTitle(), "published in", r.getCitingEntity().getVenue(), "on", r.getCitingEntity().getPublicationDate())
    result_q5 = que.getCitationsOfBibEntityByTitleWithinDate("Machine learning", "2010-01", "2024-12")
    print("Result Q5:")
    for r in result_q5:
        print(r.getCitingEntity().getTitle(), "cites", r.getCitedEntity().getTitle(), "published in", r.getCitingEntity().getVenue(), "on", r.getCitingEntity().getPublicationDate())
    result_q6 = que.getReferencesOfBibEntityByTitleWithinTimespan("Machine learning", "P2Y", "P18Y")
    print("Result Q6:")
    for r in result_q6:
        print(r.getCitingEntity().getTitle(), "is cited by", r.getCitedEntity().getTitle(), "published in", r.getCitedEntity().getVenue(), "on", r.getCitedEntity().getPublicationDate())
if __name__ == "__main__":
    main()
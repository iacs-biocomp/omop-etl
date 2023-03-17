from models.Base import Base
from models.models import Note

class Notes(Base):
    """   This class transform and insert data from CIAP to CIE 10
    """
    def __init__(self, cache):
        self.model=Note
        self.domain='Note'
        self.caches=cache

    def cleanData(self, df, module_name):
        print("\t Cleaning Data %s"%self.domain)

        df=self.reviewErrors(df, 'person_id', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'note_date', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'note_text', module_name,dropErrors=True)
        df=self.reviewErrors(df, 'note_class_concept_id', module_name, dropErrors=False)
        
       
        return df

    def mapData(self, df):
        lastPK=self.getLastPK()
        df['note_id']=range(lastPK+1,lastPK+1+len(df))
        
        
        df['note_date']=df['start_timestamp'].dt.date
        df['note_type_concept_id']=df['type_concept_id']

        return super().mapData(df)

    def insertData(self, df):
        return super().insertData(df)
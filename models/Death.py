from models.Base import Base
from models.models import t_death 

class Death(Base):
    """ This contains the clinical event for how and when Person dies
    Also, the class identifiy if there is duplicated with person_id and  format errors with the death_date 
    """
    def __init__(self, caches):
        self.model=t_death
        self.domain='Death'
        self.caches=caches
        
    def mapData(self, df):
        return super().mapData(df)

    def cleanData(self, df, module_name):
     
        df=self.reviewErrors(df, 'person_id', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'death_date', module_name, dropErrors=True)
        
        
        checkList=[('person_id', True),('condition_start_date', True) ,('condition_start_date', False)]
       
        return df

    def insertData(self, df):
        return super().insertData(df)
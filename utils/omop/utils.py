


def mapConcepts(self, df, vocabularios=[]):
    """This method is mapping the vocabulary in cache """  

    vocab=vocabularios.pop()
    self.mapConceptID(df, vocab)

    for vocab in vocabularios:
        df=self.mapConceptID(df,vocab)
    return df

def mapConceptID(self, df, vocab):
    """ This method is mapping if the target_domain_id has the same domain as the table imported"""  
    
    domain=self.domain.lower()

    if vocab is not None:
        domainVocab=self.getVocab(vocab)    

        if 'SOURCE_CODE' in df:
            """ Mapping source_code from any table by domain""" 
            df.drop(labels=domainVocab.keys(), axis=1, inplace=True)
        df=df.merge(domainVocab, left_on=domain,  right_on='SOURCE_CODE', how= 'left') 


    df['%s_concept_id'%domain]=df['%s_concept_id'%domain].fillna(df['TARGET_CONCEPT_ID'])
    
    df['%s_source_value'%domain]=df[domain]
    df['%s_source_concept_id'%domain]=df['%s_source_concept_id'%domain].fillna(df['SOURCE_CONCEPT_ID'])

    df['%s_concept_id'%domain]=df['%s_concept_id'%domain].fillna(df['TARGET_DOMAIN_ID'])
    df['domain']=df['domain'].fillna(df['TARGET_DOMAIN_ID'])

    return df

def getVocab(self, vocab):
    """ This method is getting the vocabularies mapped saved in cache"""
    
    if vocab not in self.caches:
            self.caches[vocab]=self.sourceToStandard([vocab])
    return self.caches[vocab]
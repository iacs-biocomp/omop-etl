from utils.OmopDB import OmopDB
import pandas as pd
import  config 

def sourceToStandard(sources):   #busca el vocabulario Standard 
    """ This method look for the source vocabulary and maps it to Standard vocabulary"""

    query="""select * from (
                    SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID,c.concept_name AS SOURCE_NAME, c.vocabulary_id as SOURCE_VOCABULARY_id, c1.concept_id AS TARGET_CONCEPT_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.standard_concept As TARGET_STANDARD_CONCEPT, cr2.concept_id_2 as TARGET_VALUE
                    FROM @vocabulary.CONCEPT C
                            JOIN @vocabulary.CONCEPT_RELATIONSHIP CR
                                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
                                        AND CR.invalid_reason IS NULL
                                        AND lower(cr.relationship_id) = 'maps to'
                            JOIN @vocabulary.CONCEPT C1
                                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
                                        AND C1.INVALID_REASON IS NULL
                            left join @vocabulary.CONCEPT_RELATIONSHIP CR2
                                        ON C.CONCEPT_ID = CR2.CONCEPT_ID_1
                                        AND CR2.invalid_reason IS NULL 
                                        AND lower(cr2.relationship_id) = 'maps to value'
                    UNION
                    SELECT source_code, SOURCE_CONCEPT_ID,stcm.source_code_description, source_vocabulary_id, target_concept_id, c3.domain_id AS TARGET_DOMAIN_ID, c3.standard_concept As TARGET_STANDARD_CONCEPT, null as TARGET_VALUE
                    FROM @cdm.source_to_concept_map stcm
                            LEFT OUTER JOIN @vocabulary.CONCEPT c1
                                    ON c1.concept_id = stcm.source_concept_id
                            LEFT OUTER JOIN @vocabulary.CONCEPT c2
                                    ON c2.CONCEPT_ID = stcm.target_concept_id
                            JOIN @vocabulary.CONCEPT_RELATIONSHIP CR
                                        ON C2.CONCEPT_ID = CR.CONCEPT_ID_1
                                        AND CR.invalid_reason IS NULL
                                        AND lower(cr.relationship_id) = 'maps to'
                            JOIN @vocabulary.CONCEPT C3
                                        ON CR.CONCEPT_ID_2 = C3.CONCEPT_ID
                                        AND C3.INVALID_REASON IS NULL
                    WHERE stcm.INVALID_REASON IS NULL 
                ) c
                where TARGET_STANDARD_CONCEPT='S' and SOURCE_VOCABULARY_ID= '%s';
        """%sources[0]
    
    db=OmopDB()
    conn = db.getSession()
    df=pd.read_sql_query(query.replace('@cdm', config.omop_cdm_schema).replace('@vocabulary', config.omop_vocabulary_schema), conn )
        
        
    df.columns = [x.upper() for x in df.columns]
    df['TARGET_CONCEPT_ID']=df['TARGET_CONCEPT_ID'].astype(int)
        
    return df

def getfullVocab(vocabulary):   #busca el vocabulario Standard 
    """ This method looks for the Standard vocabulary"""

    query="select * FROM @vocabulary.CONCEPT C where vocabulary_id='%s'"%vocabulary
                      
    
    db=OmopDB()
    conn = db.getSession()
    df=pd.read_sql_query(query.replace('@vocabulary', config.omop_vocabulary_schema), conn )
        
        
    df.columns = [x.upper() for x in df.columns]
    
        
    return df
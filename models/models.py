# coding: utf-8
from sqlalchemy import Column, Date, DateTime, Integer, Numeric, String, Table, Text, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

""" This script define the the class of the ETL and the structure of the columns of each table
"""
t_attribute_definition = Table(
    'attribute_definition', metadata,
    Column('attribute_definition_id', Integer, nullable=False),
    Column('attribute_name', String(255), nullable=False),
    Column('attribute_description', Text),
    Column('attribute_type_concept_id', Integer, nullable=False),
    Column('attribute_syntax', Text),
    schema='omop'
)


class CareSite(Base):
    __tablename__ = 'care_site'
    __table_args__ = {'schema': 'omop'}

    care_site_id = Column(Integer, primary_key=True, index=True)
    care_site_name = Column(String(255))
    place_of_service_concept_id = Column(Integer)
    location_id = Column(Integer)
    care_site_source_value = Column(String(50))
    place_of_service_source_value = Column(String(50))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


t_cdm_source = Table(
    'cdm_source', metadata,
    Column('cdm_source_name', String(255), nullable=False),
    Column('cdm_source_abbreviation', String(25)),
    Column('cdm_holder', String(255)),
    Column('source_description', Text),
    Column('source_documentation_reference', String(255)),
    Column('cdm_etl_reference', String(255)),
    Column('source_release_date', Date),
    Column('cdm_release_date', Date),
    Column('cdm_version', String(10)),
    Column('vocabulary_version', String(20)),
    schema='omop'
)


t_cohort_definition = Table(
    'cohort_definition', metadata,
    Column('cohort_definition_id', Integer, nullable=False),
    Column('cohort_definition_name', String(255), nullable=False),
    Column('cohort_definition_description', Text),
    Column('definition_type_concept_id', Integer, nullable=False),
    Column('cohort_definition_syntax', Text),
    Column('subject_concept_id', Integer, nullable=False),
    Column('cohort_initiation_date', Date),
    schema='omop'
)


class Concept(Base):
    __tablename__ = 'concept'
    __table_args__ = {'schema': 'omop'}

    concept_id = Column(Integer, primary_key=True, index=True)
    concept_name = Column(String(255), nullable=False)
    domain_id = Column(String(20), nullable=False, index=True)
    vocabulary_id = Column(String(20), nullable=False, index=True)
    concept_class_id = Column(String(20), nullable=False, index=True)
    standard_concept = Column(String(1))
    concept_code = Column(String(50), nullable=False, index=True)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1))


t_concept_ancestor = Table(
    'concept_ancestor', metadata,
    Column('ancestor_concept_id', Integer, nullable=False, index=True),
    Column('descendant_concept_id', Integer, nullable=False, index=True),
    Column('min_levels_of_separation', Integer, nullable=False),
    Column('max_levels_of_separation', Integer, nullable=False),
    schema='omop'
)


class ConceptClas(Base):
    __tablename__ = 'concept_class'
    __table_args__ = {'schema': 'omop'}

    concept_class_id = Column(String(20), primary_key=True, index=True)
    concept_class_name = Column(String(255), nullable=False)
    concept_class_concept_id = Column(Integer, nullable=False)


t_concept_relationship = Table(
    'concept_relationship', metadata,
    Column('concept_id_1', Integer, nullable=False, index=True),
    Column('concept_id_2', Integer, nullable=False, index=True),
    Column('relationship_id', String(20), nullable=False, index=True),
    Column('valid_start_date', Date, nullable=False),
    Column('valid_end_date', Date, nullable=False),
    Column('invalid_reason', String(1)),
    schema='omop'
)


t_concept_synonym = Table(
    'concept_synonym', metadata,
    Column('concept_id', Integer, nullable=False, index=True),
    Column('concept_synonym_name', String(1000), nullable=False),
    Column('language_concept_id', Integer, nullable=False),
    schema='omop'
)


class ConditionEra(Base):
    __tablename__ = 'condition_era'
    __table_args__ = {'schema': 'omop'}

    condition_era_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    condition_concept_id = Column(Integer, nullable=False, index=True)
    condition_era_start_date = Column(DateTime, nullable=False)
    condition_era_end_date = Column(DateTime, nullable=False)
    condition_occurrence_count = Column(Integer)


class ConditionOccurrence(Base):
    __tablename__ = 'condition_occurrence'
    __table_args__ = {'schema': 'omop'}

    condition_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    condition_concept_id = Column(Integer, nullable=False, index=True)
    condition_start_date = Column(Date, nullable=False)
    condition_start_datetime = Column(DateTime)
    condition_end_date = Column(Date)
    condition_end_datetime = Column(DateTime)
    condition_type_concept_id = Column(Integer, nullable=False)
    condition_status_concept_id = Column(Integer)
    stop_reason = Column(String(20))
    provider_id = Column(Integer)
    visit_occurrence_id = Column(Integer, index=True)
    visit_detail_id = Column(Integer)
    condition_source_value = Column(String(50))
    condition_source_concept_id = Column(Integer)
    condition_status_source_value = Column(String(50))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class Cost(Base):
    __tablename__ = 'cost'
    __table_args__ = {'schema': 'omop'}

    cost_id = Column(Integer, primary_key=True)
    cost_event_id = Column(Integer, nullable=False, index=True)
    cost_domain_id = Column(String(20), nullable=False)
    cost_type_concept_id = Column(Integer, nullable=False)
    currency_concept_id = Column(Integer)
    total_charge = Column(Numeric)
    total_cost = Column(Numeric)
    total_paid = Column(Numeric)
    paid_by_payer = Column(Numeric)
    paid_by_patient = Column(Numeric)
    paid_patient_copay = Column(Numeric)
    paid_patient_coinsurance = Column(Numeric)
    paid_patient_deductible = Column(Numeric)
    paid_by_primary = Column(Numeric)
    paid_ingredient_cost = Column(Numeric)
    paid_dispensing_fee = Column(Numeric)
    payer_plan_period_id = Column(Integer)
    amount_allowed = Column(Numeric)
    revenue_code_concept_id = Column(Integer)
    revenue_code_source_value = Column(String(50))
    drg_concept_id = Column(Integer)
    drg_source_value = Column(String(3))


t_death = Table(
    'death', metadata,
    Column('person_id', Integer, nullable=False, index=True),
    Column('death_date', Date, nullable=False),
    Column('death_datetime', DateTime),
    Column('death_type_concept_id', Integer),
    Column('cause_concept_id', Integer),
    Column('cause_source_value', String(50)),
    Column('cause_source_concept_id', Integer),
    Column('origin_source_data_provider', String(255)),
    Column('origin_source_name', String(255)),
    Column('origin_source_value', String(255)),
    schema='omop'
)

t_death.__tablename__ = 'death'

class DeviceExposure(Base):
    __tablename__ = 'device_exposure'
    __table_args__ = {'schema': 'omop'}

    device_exposure_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    device_concept_id = Column(Integer, nullable=False, index=True)
    device_exposure_start_date = Column(Date, nullable=False)
    device_exposure_start_datetime = Column(DateTime)
    device_exposure_end_date = Column(Date)
    device_exposure_end_datetime = Column(DateTime)
    device_type_concept_id = Column(Integer, nullable=False)
    unique_device_id = Column(String(50))
    quantity = Column(Integer)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(Integer, index=True)
    visit_detail_id = Column(Integer)
    device_source_value = Column(String(50))
    device_source_concept_id = Column(Integer)
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class Domain(Base):
    __tablename__ = 'domain'
    __table_args__ = {'schema': 'omop'}

    domain_id = Column(String(20), primary_key=True, index=True)
    domain_name = Column(String(255), nullable=False)
    domain_concept_id = Column(Integer, nullable=False)


class DoseEra(Base):
    __tablename__ = 'dose_era'
    __table_args__ = {'schema': 'omop'}

    dose_era_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    drug_concept_id = Column(Integer, nullable=False, index=True)
    unit_concept_id = Column(Integer, nullable=False)
    dose_value = Column(Numeric, nullable=False)
    dose_era_start_date = Column(DateTime, nullable=False)
    dose_era_end_date = Column(DateTime, nullable=False)


class DrugEra(Base):
    __tablename__ = 'drug_era'
    __table_args__ = {'schema': 'omop'}

    drug_era_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    drug_concept_id = Column(Integer, nullable=False, index=True)
    drug_era_start_date = Column(DateTime, nullable=False)
    drug_era_end_date = Column(DateTime, nullable=False)
    drug_exposure_count = Column(Integer)
    gap_days = Column(Integer)


class DrugExposure(Base):
    __tablename__ = 'drug_exposure'
    __table_args__ = {'schema': 'omop'}

    drug_exposure_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    drug_concept_id = Column(Integer, nullable=False, index=True)
    drug_exposure_start_date = Column(Date, nullable=False)
    drug_exposure_start_datetime = Column(DateTime)
    drug_exposure_end_date = Column(Date, nullable=False)
    drug_exposure_end_datetime = Column(DateTime)
    verbatim_end_date = Column(Date)
    drug_type_concept_id = Column(Integer, nullable=False)
    stop_reason = Column(String(20))
    refills = Column(Integer)
    quantity = Column(Numeric)
    days_supply = Column(Integer)
    sig = Column(Text)
    route_concept_id = Column(Integer)
    lot_number = Column(String(50))
    provider_id = Column(Integer)
    visit_occurrence_id = Column(Integer, index=True)
    visit_detail_id = Column(Integer)
    drug_source_value = Column(String(50))
    drug_source_concept_id = Column(Integer)
    route_source_value = Column(String(50))
    dose_unit_source_value = Column(String(50))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


t_drug_strength = Table(
    'drug_strength', metadata,
    Column('drug_concept_id', Integer, nullable=False, index=True),
    Column('ingredient_concept_id', Integer, nullable=False, index=True),
    Column('amount_value', Numeric),
    Column('amount_unit_concept_id', Integer),
    Column('numerator_value', Numeric),
    Column('numerator_unit_concept_id', Integer),
    Column('denominator_value', Numeric),
    Column('denominator_unit_concept_id', Integer),
    Column('box_size', Integer),
    Column('valid_start_date', Date, nullable=False),
    Column('valid_end_date', Date, nullable=False),
    Column('invalid_reason', String(1)),
    schema='omop'
)


t_fact_relationship = Table(
    'fact_relationship', metadata,
    Column('domain_concept_id_1', Integer, nullable=False, index=True),
    Column('fact_id_1', Integer, nullable=False),
    Column('domain_concept_id_2', Integer, nullable=False, index=True),
    Column('fact_id_2', Integer, nullable=False),
    Column('relationship_concept_id', Integer, nullable=False, index=True),
    Column('origin_source_data_provider', String(255)),
    Column('origin_source_name', String(255)),
    Column('origin_source_value', String(255)),
    schema='omop'
)

t_fact_relationship.__tablename__ = 'fact_relationship'



class Location(Base):
    __tablename__ = 'location'
    __table_args__ = {'schema': 'omop'}

    location_id = Column(Integer, primary_key=True, index=True)
    address_1 = Column(String(50))
    address_2 = Column(String(50))
    city = Column(String(50))
    state = Column(String(2))
    zip = Column(String(9))
    county = Column(String(20))
    location_source_value = Column(String(50))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class Measurement(Base):
    __tablename__ = 'measurement'
    __table_args__ = {'schema': 'omop'}

    measurement_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    measurement_concept_id = Column(Integer, nullable=False, index=True)
    measurement_date = Column(Date, nullable=False)
    measurement_datetime = Column(DateTime)
    measurement_time = Column(String(10))
    measurement_type_concept_id = Column(Integer, nullable=False)
    operator_concept_id = Column(Integer)
    value_as_number = Column(Numeric)
    value_as_concept_id = Column(Integer)
    unit_concept_id = Column(Integer)
    range_low = Column(Numeric)
    range_high = Column(Numeric)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(Integer, index=True)
    visit_detail_id = Column(Integer)
    measurement_source_value = Column(String(50))
    measurement_source_concept_id = Column(Integer)
    unit_source_value = Column(String(50))
    value_source_value = Column(String(50))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


t_metadata_ = Table(
    'metadata', metadata,
    Column('metadata_concept_id', Integer, nullable=False, index=True),
    Column('metadata_type_concept_id', Integer, nullable=False),
    Column('name', String(250), nullable=False),
    Column('value_as_string', String(250)),
    Column('value_as_concept_id', Integer),
    Column('metadata_date', Date),
    Column('metadata_datetime', DateTime),
    schema='omop'
)


class Note(Base):
    __tablename__ = 'note'
    __table_args__ = {'schema': 'omop'}

    note_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    note_date = Column(Date, nullable=False)
    note_datetime = Column(DateTime)
    note_type_concept_id = Column(Integer, nullable=False, index=True)
    note_class_concept_id = Column(Integer, nullable=False)
    note_title = Column(String(250))
    note_text = Column(Text, nullable=False)
    encoding_concept_id = Column(Integer, nullable=False)
    language_concept_id = Column(Integer, nullable=False)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(Integer, index=True)
    visit_detail_id = Column(Integer)
    note_source_value = Column(String(50))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class NoteNlp(Base):
    __tablename__ = 'note_nlp'
    __table_args__ = {'schema': 'omop'}

    note_nlp_id = Column(Integer, primary_key=True)
    note_id = Column(Integer, nullable=False, index=True)
    section_concept_id = Column(Integer)
    snippet = Column(String(250))
    offset = Column(String(50))
    lexical_variant = Column(String(250), nullable=False)
    note_nlp_concept_id = Column(Integer, index=True)
    note_nlp_source_concept_id = Column(Integer)
    nlp_system = Column(String(250))
    nlp_date = Column(Date, nullable=False)
    nlp_datetime = Column(DateTime)
    term_exists = Column(String(1))
    term_temporal = Column(String(50))
    term_modifiers = Column(String(2000))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class Observation(Base):
    __tablename__ = 'observation'
    __table_args__ = {'schema': 'omop'}

    observation_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    observation_concept_id = Column(Integer, nullable=False, index=True)
    observation_date = Column(Date, nullable=False)
    observation_datetime = Column(DateTime)
    observation_type_concept_id = Column(Integer, nullable=False)
    value_as_number = Column(Numeric)
    value_as_string = Column(String(60))
    value_as_concept_id = Column(Integer)
    qualifier_concept_id = Column(Integer)
    unit_concept_id = Column(Integer)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(Integer, index=True)
    visit_detail_id = Column(Integer)
    observation_source_value = Column(String(50))
    observation_source_concept_id = Column(Integer)
    unit_source_value = Column(String(50))
    qualifier_source_value = Column(String(50))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class ObservationPeriod(Base):
    __tablename__ = 'observation_period'
    __table_args__ = {'schema': 'omop'}

    observation_period_id = Column(Integer, primary_key=True, server_default=text("nextval('omop.observation_period_observation_period_id_seq'::regclass)"))
    person_id = Column(Integer, nullable=False, index=True)
    observation_period_start_date = Column(Date, nullable=False)
    observation_period_end_date = Column(Date, nullable=False)
    period_type_concept_id = Column(Integer, nullable=False)


class PayerPlanPeriod(Base):
    __tablename__ = 'payer_plan_period'
    __table_args__ = {'schema': 'omop'}

    payer_plan_period_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    payer_plan_period_start_date = Column(Date, nullable=False)
    payer_plan_period_end_date = Column(Date, nullable=False)
    payer_concept_id = Column(Integer)
    payer_source_value = Column(String(50))
    payer_source_concept_id = Column(Integer)
    plan_concept_id = Column(Integer)
    plan_source_value = Column(String(50))
    plan_source_concept_id = Column(Integer)
    sponsor_concept_id = Column(Integer)
    sponsor_source_value = Column(String(50))
    sponsor_source_concept_id = Column(Integer)
    family_source_value = Column(String(50))
    stop_reason_concept_id = Column(Integer)
    stop_reason_source_value = Column(String(50))
    stop_reason_source_concept_id = Column(Integer)


class Person(Base):
    __tablename__ = 'person'
    __table_args__ = {'schema': 'omop'}

    person_id = Column(Integer, primary_key=True, index=True)
    gender_concept_id = Column(Integer, nullable=False, index=True)
    year_of_birth = Column(Integer, nullable=False)
    month_of_birth = Column(Integer)
    day_of_birth = Column(Integer)
    birth_datetime = Column(DateTime)
    race_concept_id = Column(Integer, nullable=False)
    ethnicity_concept_id = Column(Integer, nullable=False)
    location_id = Column(Integer)
    provider_id = Column(Integer)
    care_site_id = Column(Integer)
    person_source_value = Column(String(50))
    gender_source_value = Column(String(50))
    gender_source_concept_id = Column(Integer)
    race_source_value = Column(String(50))
    race_source_concept_id = Column(Integer)
    ethnicity_source_value = Column(String(50))
    ethnicity_source_concept_id = Column(Integer)
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class ProcedureOccurrence(Base):
    __tablename__ = 'procedure_occurrence'
    __table_args__ = {'schema': 'omop'}

    procedure_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    procedure_concept_id = Column(Integer, nullable=False, index=True)
    procedure_date = Column(Date, nullable=False)
    procedure_datetime = Column(DateTime)
    procedure_type_concept_id = Column(Integer, nullable=False)
    modifier_concept_id = Column(Integer)
    quantity = Column(Integer)
    provider_id = Column(Integer)
    visit_occurrence_id = Column(Integer, index=True)
    visit_detail_id = Column(Integer)
    procedure_source_value = Column(String(50))
    procedure_source_concept_id = Column(Integer)
    modifier_source_value = Column(String(50))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class Provider(Base):
    __tablename__ = 'provider'
    __table_args__ = {'schema': 'omop'}

    provider_id = Column(Integer, primary_key=True, index=True)
    provider_name = Column(String(255))
    npi = Column(String(20))
    dea = Column(String(20))
    specialty_concept_id = Column(Integer)
    care_site_id = Column(Integer)
    year_of_birth = Column(Integer)
    gender_concept_id = Column(Integer)
    provider_source_value = Column(String(50))
    specialty_source_value = Column(String(50))
    specialty_source_concept_id = Column(Integer)
    gender_source_value = Column(String(50))
    gender_source_concept_id = Column(Integer)
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class Relationship(Base):
    __tablename__ = 'relationship'
    __table_args__ = {'schema': 'omop'}

    relationship_id = Column(String(20), primary_key=True, index=True)
    relationship_name = Column(String(255), nullable=False)
    is_hierarchical = Column(String(1), nullable=False)
    defines_ancestry = Column(String(1), nullable=False)
    reverse_relationship_id = Column(String(20), nullable=False)
    relationship_concept_id = Column(Integer, nullable=False)


t_source_to_concept_map = Table(
    'source_to_concept_map', metadata,
    Column('source_code', String(50), nullable=False, index=True),
    Column('source_concept_id', Integer, nullable=False),
    Column('source_vocabulary_id', String(20), nullable=False, index=True),
    Column('source_code_description', String(255)),
    Column('target_concept_id', Integer, nullable=False, index=True),
    Column('target_vocabulary_id', String(20), nullable=False, index=True),
    Column('valid_start_date', Date, nullable=False),
    Column('valid_end_date', Date, nullable=False),
    Column('invalid_reason', String(1)),
    schema='omop'
)
t_source_to_concept_map.__tablename__ = 'source_to_concept_map'

class Speciman(Base):
    __tablename__ = 'specimen'
    __table_args__ = {'schema': 'omop'}

    specimen_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    specimen_concept_id = Column(Integer, nullable=False, index=True)
    specimen_type_concept_id = Column(Integer, nullable=False)
    specimen_date = Column(Date, nullable=False)
    specimen_datetime = Column(DateTime)
    quantity = Column(Numeric)
    unit_concept_id = Column(Integer)
    anatomic_site_concept_id = Column(Integer)
    disease_status_concept_id = Column(Integer)
    specimen_source_id = Column(String(50))
    specimen_source_value = Column(String(50))
    unit_source_value = Column(String(50))
    anatomic_site_source_value = Column(String(50))
    disease_status_source_value = Column(String(50))
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class VisitDetail(Base):
    __tablename__ = 'visit_detail'
    __table_args__ = {'schema': 'omop'}

    visit_detail_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    visit_detail_concept_id = Column(Integer, nullable=False, index=True)
    visit_detail_start_date = Column(Date, nullable=False)
    visit_detail_start_datetime = Column(DateTime)
    visit_detail_end_date = Column(Date, nullable=False)
    visit_detail_end_datetime = Column(DateTime)
    visit_detail_type_concept_id = Column(Integer, nullable=False)
    provider_id = Column(Integer)
    care_site_id = Column(Integer)
    visit_detail_source_value = Column(String(50))
    visit_detail_source_concept_id = Column(Integer)
    admitting_source_value = Column(String(50))
    admitting_source_concept_id = Column(Integer)
    discharge_to_source_value = Column(String(50))
    discharge_to_concept_id = Column(Integer)
    preceding_visit_detail_id = Column(Integer)
    visit_detail_parent_id = Column(Integer)
    visit_occurrence_id = Column(Integer, nullable=False, index=True)
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class VisitOccurrence(Base):
    __tablename__ = 'visit_occurrence'
    __table_args__ = {'schema': 'omop'}

    visit_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    visit_concept_id = Column(Integer, nullable=False, index=True)
    visit_start_date = Column(Date, nullable=False)
    visit_start_datetime = Column(DateTime)
    visit_end_date = Column(Date, nullable=False)
    visit_end_datetime = Column(DateTime)
    visit_type_concept_id = Column(Integer, nullable=False)
    provider_id = Column(Integer)
    care_site_id = Column(Integer)
    visit_source_value = Column(String(50))
    visit_source_concept_id = Column(Integer)
    admitting_source_concept_id = Column(Integer)
    admitting_source_value = Column(String(50))
    discharge_to_concept_id = Column(Integer)
    discharge_to_source_value = Column(String(50))
    preceding_visit_occurrence_id = Column(Integer)
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class VisitOccurrence2(Base):
    __tablename__ = 'visit_occurrence2'
    __table_args__ = {'schema': 'omop'}

    visit_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False, index=True)
    visit_concept_id = Column(Integer, nullable=False, index=True)
    visit_start_date = Column(Date, nullable=False)
    visit_start_datetime = Column(DateTime)
    visit_end_date = Column(Date, nullable=False)
    visit_end_datetime = Column(DateTime)
    visit_type_concept_id = Column(Integer, nullable=False)
    provider_id = Column(Integer)
    care_site_id = Column(Integer)
    visit_source_value = Column(String(50))
    visit_source_concept_id = Column(Integer)
    admitting_source_concept_id = Column(Integer)
    admitting_source_value = Column(String(50))
    discharge_to_concept_id = Column(Integer)
    discharge_to_source_value = Column(String(50))
    preceding_visit_occurrence_id = Column(Integer)
    origin_source_data_provider = Column(String(255))
    origin_source_name = Column(String(255))
    origin_source_value = Column(String(255))


class Vocabulary(Base):
    __tablename__ = 'vocabulary'
    __table_args__ = {'schema': 'omop'}

    vocabulary_id = Column(String(20), primary_key=True, index=True)
    vocabulary_name = Column(String(255), nullable=False)
    vocabulary_reference = Column(String(255), nullable=False)
    vocabulary_version = Column(String(255))
    vocabulary_concept_id = Column(Integer, nullable=False)

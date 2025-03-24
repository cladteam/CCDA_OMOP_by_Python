
from numpy import datetime64
from numpy import float64
from numpy import float32
from numpy import int32 # int
from numpy import int64 # long

domain_dataframe_column_types ={
 
    'care_site':{
        'care_site_id': int64, # check
        'place_of_service_concept_id': int32,
        'care_site_source_value': str,
        'place_of_service_source_value': str
    },
    #'location':{
    #}
    'provider': {
        'gender_concept_id': int32,
        'specialty_concept_id': int32,
        'specialty_source_concept_id': int32,
        'gender_source_concept_id' : int32,
        'dea': str,
        'npi': str
    },
    'person': {
        'race_source_concept_id' : int32,
        'race_source_value' : str,
        'ethnicity_source_concept_id' : int32,
        'ethnicity_source_value' : str,
        'gender_source_concept_id' : int32,
        'gender_source_value' : str,
        'provider_id' : int64,
        
        'gender_concept_id' : int32,
        'ethnicity_concept_id' : int32,
        'race_concept_id' : int32
    },
    'visit_occurrence': {
        'admitting_source_concept_id': int32,
        'care_site_id': int64, # check
        'discharge_to_concept_id': int32,
        'visit_source_concept_id': int32, 
        'preceding_visit_occurrence_id': int64,
        'visit_type_concept_id': int32,
        'visit_concept_id': int32,
    },
    'measurement': {
        'operator_concept_id': int32,
        'value_as_concept_id': int32,
        'unit_concept_id': int32,
        'measurement_source_concept_id': int32,
        'measurement_datetime': "datetime",
        'measurement_date': "date",
        'measurement_time': str,
        'range_low' : float32,
        'range_high':float32,
        'provider_id': int64,
        'visit_occurrence_id': int64,
        'visit_detail_id': int64,

        'measurement_type_concept_id': int32, 
        'value_as_number': float64
    },
    'observation': {
        'value_as_number': float32,
        'qualifier_concept_id': int32,
        'unit_concept_id': int32,
        'observation_source_concept_id': int32,
        'observation_datetime': "datetime",
        'observation_date': datetime64,
        'provider_id': int64,
        'visit_occurrence_id': int64,
        'visit_detail_id': int64,
        'observation_type_concept_id': int32,

        'value_as_concept_id': int32,
        'value_as_number': float64
    },
    'condition_occurrence': {
        'visit_occurrence_id': int64,
        'condition_status_concept_id': int32,
        'condition_source_concept_id': int32,
        'condition_end_datetime': "datetime",
        'condition_end_date': "date",
        'condition_status_source_value': str,
        'provider_id': int64,
        'visit_occurrence_id': int64,
        'visit_detail_id': int64,

        'condition_type_concept_id': int32,
    },
    'procedure_occurrence': {
        'visit_occurrence_id': int64,
        'modifier_concept_id': int32,
        'modifier_source_value': str,
        'procedure_source_concept_id': int32,
        'provider_id': int64,
        'visit_occurrence_id': int64,
        'visit_detail_id': int64,

        'procedure_type_concept_id': int32,
        'procedure_concept_id': int32,
        'procedure_source_concept_id': int32
    },
    'drug_exposure': {
        'quantity': float32,
        'sig' : str,
        'route_concept_id': int32,
        'route_source_value': str,
        'lot_number': str,
        'visit_occurrence_id': int64,
        'provider_id': int64,
        'visit_occurrence_id': int64,
        'visit_detail_id': int64
    }
    
}

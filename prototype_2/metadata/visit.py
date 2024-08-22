
import prototype_2.value_transformations as VT

metadata = {
    'Visit': {
    	# FIX: there's a code for what might be admitting diagnosis here
    	'root': {
    	    'output': False,
    	    'config_type': 'ROOT',
    	    'element': "./componentOf/encompassingEncounter"
    	},
    	'person_id': {
    	    'output': True,
    	    'config_type': 'FK',
    	    'FK': 'person_id'
    	},
    	'visit_occurrence_id_170': {   # for the 170.314...file
    	    # FIX: why would an occurence_id be an NPI????!!!!!!!
    	    'output': False,
    	    'config_type': 'PK',
    	    'element': 'id[@root="1.3.6.1.4.1.42424242.4.99930.4.3.4"]',
    		# The root says "NPI". The extension is the actual NPI
    	    'attribute': "extension",
    	},
    	'visit_occurrence_id': {
    	    # FIX: why would an occurence_id be an NPI????!!!!!!!
    	    'output': True,
    	    'config_type': 'PK',
    	    'element': 'id[@root="2.16.840.1.113883.4.6"]',
    		# The root says "NPI". The extension is the actual NPI
    	    'attribute': "extension",
    	},
    	'visit_concept_code': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "code",	 # FIX ToDo is this what I think it is?,
    	    'attribute': "code"
    	},
    	'visit_concept_codeSystem': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "code",
    	    'attribute': "codeSystem"
    	},
    	'visit_concept_id': {
    	    'output': True,
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'visit_concept_code',
    		    'vocabulary_oid': 'visit_concept_codeSystem'
    	    }
    	},
    	'care_site_id': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "location/healthCareFacility/id",
    	    'attribute': "root"
    	},
    	# FIX TODO sometimes a document will have more than one encounterParticipant. The way this is configured, they will be awkwardly merged.
    	'provider_id_ep_170': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': 'encounterParticipant/assignedEntity/id[@root="1.3.6.1.4.1.42424242.4.99930.4"]',
    	    'attribute': "extension"
    	},
    	'provider_id_ep_npi_170': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': 'encounterParticipant/assignedEntity/id[@root="2.16.840.1.113883.4.6"]',
    	    'attribute': "extension"
    	},
    	'provider_id': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "responsibleParty/assignedEntity/id",
    	    'attribute': "root"
    	},
    	# leaving these here more for testing how to pull #text
    	'provider_prefix_ep': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "encounterParticipant/assignedEntity/assignedPerson/name/prefix",
    	    'attribute': "#text"
    	},
    	'provider_given_ep': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "encounterParticipant/assignedEntity/assignedPerson/name/given",
    	    'attribute': "#text"
    	},
    	'provider_family_ep': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "encounterParticipant/assignedEntity/assignedPerson/name/family",
    	    'attribute': "#text"
    	},
    	'provider_suffix_ep': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "encounterParticipant/assignedEntity/assignedPerson/name/suffix",
    	    'attribute': "#text"
    	},
    	# leaving these here more for testing how to pull #text
    	'provider_prefix': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "responsibleParty/assignedEntity/assignedPerson/name/prefix",
    	    'attribute': "#text"
    	},
    	'provider_given': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "responsibleParty/assignedEntity/assignedPerson/name/given",
    	    'attribute': "#text"
    	},
    	'provider_family': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "responsibleParty/assignedEntity/assignedPerson/name/family",
    	    'attribute': "#text"
    	},
    	# FIX is it consistenly a high/low pair? do we sometimes get just effectiveTime@value ?
    	'start_time': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "effectiveTime/low",
    	    'attribute': "value"
    	},
    	'end_time':  {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "effectiveTime/high",
    	    'attribute': "value"
    	}
    }
}

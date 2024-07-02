
"""
    A stub for a proper vocabulary lookup facility.
    Also some convenient template id constants from the CCDA standards

    Maps HL7 codeSystem OIDs to more/less equivalent OMOP vocabulary_id
    Only more/less because OMOP doesn't track versions and so I think this
    could be a many-to-one mapping where multiple versions in HL7 map to
    whatever is in OMOP.
    Concepts in vocabularies here only need to be mapped from concept_code to concept_id.
"""
# document template IDs
US_GENERAL_ROOT = "2.16.840.1.113883.10.20.22.1.1"
CCD_DOCUMENT_ROOT = "2.16.840.1.113883.10.20.22.1.2"  # I think.


# HL7: codeSyste, code --> OMOP: vocabulary_id, concept_code, name, concept_id
complex_mappings = {
   ('2.16.840.1.113883.5.1', 'F'): ("Gender", 'F', "FEMALE", 8532),
   ('2.16.840.1.113883.5.1', 'M'): ("Gender", 'M', "MALE", 8532),

   #  ("urn:oid:2.16.840.1.113883.6.238", "2106-3"): ("Race", "5", "White", 8527),
   ("2.16.840.1.113883.6.238", "2106-3"): ("Race", "5", "White", 8527),
   ("2.16.840.1.113883.6.238", None): ("Race", "1", "American Indian or Alaskan Native", 8657),
   #  ("2.16.840.1.113883.6.238", None): ("Race", "2", "Asian", 8515),
   #  ("2.16.840.1.113883.6.238", None): ("Race", "3", "Black or Afrian American", 8516),

   #  ("urn:oid:2.16.840.1.113883.6.238", "2186-5"):
   #       ("Ethnicity", "Not Hispanic", "Not Hispanic or Latino", 38003564),
   ("2.16.840.1.113883.6.238", "2186-5"): ("Ethnicity", "Not Hispanic",
                                           "Not Hispanic or Latino", 38003564)
   # ("2.16.840.1.113883.6.238", None):
   #    ("Ethnicity", "Hispanic", "Hispanic or Latino", 9998, 38003563)
}

# possible "domain" sections under this (prefix omitted) path:
# component/structuredBody/component/section/
# reason_for_referral="1.3.6.1.4.1.19376.1.5.3.1.3.1"
# medications        ="2.16.840.1.113883.10.20.22.2.1.1"
# immunizations      ="2.16.840.1.113883.10.20.22.2.2.1"
RESULTS = "2.16.840.1.113883.10.20.22.2.3.1"
# vital_signs        ="2.16.840.1.113883.10.20.22.2.4.1"
# problems           ="2.16.840.1.113883.10.20.22.2.5.1"
# allergies          ="2.16.840.1.113883.10.20.22.2.6.1"
# procedures         ="2.16.840.1.113883.10.20.22.2.7.1"
# care_plan          ="2.16.840.1.113883.10.20.22.2.10"
# functional_and_cognitive_status="2.16.840.1.113883.10.20.22.2.14"
# social_history     ="2.16.840.1.113883.10.20.22.2.17"
# encounters          ="2.16.840.1.113883.10.20.22.2.22.1"

# <typeId     root="2.16.840.1.113883.1.3" extension="POCD_HD000040"/>

# OID --> (name, omop concept_id)
oid_map = {
    '2.16.840.1.113883.1.11.19563': ('Personal Relationship Role Type Value Set', 0),
    '2.16.840.1.113883.4.1': ('SSN', 0), 
    '2.16.840.1.113883.4.6': ('NPI', 0),
    '2.16.840.1.113883.5.111': ('RoleCode', 0),
    '2.16.840.1.113883.5.1': ('XXGender', 0),
    '2.16.840.1.113883.5.4': ('ActCode', 0),
    '2.16.840.1.113883.5.6': ('ActClass', 0),
    '2.16.840.1.113883.6.1': ('LOINC', 44819139),
    '2.16.840.1.113883.6.101': ('NUCC', 44819137),
    '2.16.840.1.113883.6.12': ('CPT4', 0),
    '2.16.840.1.113883.6.259': ('HealthcareServiceLocation', 0),
    '2.16.840.1.113883.6.59': ('CVX', 0),
    '2.16.840.1.113883.6.88': ('RxNorm', 44819104),
    '2.16.840.1.113883.6.96': ('SNOMED', 32549),
    '2.16.840.1.113883.6.238': ('XXRaceEthnicity', 1),
    '2.16.840.1.113883.12.443': ('XXY', 2),
}


vocab_map = {
   # vocabuarly, id --> name, concept_id, vocabulary_id, concept_code
   (1,2) : ("foo", "bar"),
   ("http://loinc.org", "8302-2")                : ("Body Height", 3036277, "LOINC", '8302-2'),
   ("urn:oid:2.16.840.1.113883.6.238", "2106-3") : ("White", 8527, "Race", '5'),
   ("2.16.840.1.113883.6.238", "2106-3")         : ("White", 8527, "Race", '5'),
   ("urn:oid:2.16.840.1.113883.6.238", "2186-5") : ("Not Hispanic or Latino", 9998, "X", '2186-5'),
   ("2.16.840.1.113883.6.238", "2186-5")         : ("Not Hispanic or Latino", 9998, "X", '2186-5'),
   ("http://snomed.info/sct", "367336001")       : ("Chemotherapy", 4273629, "SNOMED", '367336001'),
   ("http://snomed.info/sct", "387713003")       : ("Surgical procedure", 4301351, "SNOMED", '387713003'),
   ('2.16.840.1.113883.5.1', 'F')                : ("FEMALE", 8532, "Gender", 'F' ),
   ('2.16.840.1.113883.5.1', 'M')                : ("FEMALE", 8532, "Gender", 'F' ),
   ('2.16.840.1.113883.6.1', '30313-1')          : ("Hemoglobin", 3002173, "LOINC", "30313-1"),
   ('2.16.840.1.113883.6.1', '33765-9')          : ("Leukocytes", 3028866, "LOINC", "33765-9"),
   ('2.16.840.1.113883.6.1', '26515-7')          : ("Platelets",  3007461, "LOINC", "26515-7")
}



# possible "domain" sections under this (prefix omitted) path:
# component/structuredBody/component/section/ 
reason_for_referral="1.3.6.1.4.1.19376.1.5.3.1.3.1"
medications        ="2.16.840.1.113883.10.20.22.2.1.1"
immunizations      ="2.16.840.1.113883.10.20.22.2.2.1"
results            ="2.16.840.1.113883.10.20.22.2.3.1"
vital_signs        ="2.16.840.1.113883.10.20.22.2.4.1"
problems           ="2.16.840.1.113883.10.20.22.2.5.1"
allergies          ="2.16.840.1.113883.10.20.22.2.6.1"
procedures         ="2.16.840.1.113883.10.20.22.2.7.1"
care_plan          ="2.16.840.1.113883.10.20.22.2.10"
functional_and_cognitive_status="2.16.840.1.113883.10.20.22.2.14"
social_history     ="2.16.840.1.113883.10.20.22.2.17"
encounters          ="2.16.840.1.113883.10.20.22.2.22.1"


# document template IDs
US_general_root="2.16.840.1.113883.10.20.22.1.1"
ccd_document_root = "2.16.840.1.113883.10.20.22.1.2" # I think.


## typeId?
##   <typeId     root="2.16.840.1.113883.1.3" extension="POCD_HD000040"/>
         
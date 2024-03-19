
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
   ('2.16.840.1.113883.5.1', 'M')                : ("FEMALE", 8532, "Gender", 'F' )
}


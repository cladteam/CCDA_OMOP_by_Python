#!/usr/bin/env python3

# omop_person.py : OmopPerson
#
# represents the OMOP v5.3 person table

import time

class OmopPerson:
    def __init__(self, person_id, gender_concept_id, birth_date_time, race_concept_id, ethnicity_concept_id, location_id):

        isinstance(person_id, int)
        isinstance(gender_concept_id, int)
        isinstance(race_concept_id, int)
        isinstance(ethnicity_concept_id, int)
        isinstance(location_id, int)
        isinstance(birth_date_time, str)

        # TODO, (conditionally) check these against the vocabulary and appropriate PK-FK

        (year, month, day) = self.parse_time(birth_date_time)
       
        self.person_id = person_id                   # integer not null
        self.gender_concept_id = gender_concept_id   # integer not null
        self.year_of_birth = year                    # integer NOT NULL
        self.month_of_birth = month                  # integer NULL
        self.day_of_birth = day                      # integer NULL
        self.birth_date_time = birth_date_time           # integer Null
        self.race_concept_id = race_concept_id       # integer  not null
        self.ethnicity_concept_id = ethnicity_concept_id  # integer  not null

        self.location_id = location_id # integer     # integer NULL

        self.provider_id = None # integer NULL,
        self.care_site_id = None # integer NULL,
        self.person_source_value = None # varchar(50) NULL,
        self.gender_source_value = None # varchar(50) NULL,
        self.gender_source_concept_id = None # integer NULL,
        self.race_source_value = None # varchar(50) NULL,
        self.race_source_concept_id = None # integer NULL,
        self.ethnicity_source_value = None  # varchar(50) NULL,
        self.ethnicity_source_concept_id = None #integer NULL 

    @staticmethod
    def parse_time(datetime) :
        time_struct = time.strptime(datetime, '%Y-%m-%d')
        year = time.strftime('%Y', time_struct)
        month = time.strftime('%m', time_struct)
        day = time.strftime('%d', time_struct)
        return (year, month, day)

    def create_dictionary(self):
        dest = {}
    
        dest['person_id'] = self.person_id
        dest['race_concept_id'] = self.race_concept_id
        dest['ethnicity_concept_id'] = self.ethnicity_concept_id
        dest['gender_concept_id'] = self.gender_concept_id
        dest['birth_datetime'] = self.birth_date_time
        dest['location_id'] = self.location_id
      
        return dest

    @staticmethod
    def create_header():
        print("person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth, birth_datetime, " + 
              "race_concept_id, ethnicity_concept_id, location_id, provider_id, care_site_id, person_source_value, " +
              "gender_source_value, gender_source_concpet_id,  race_source_value, race_source_concept_id, " + 
              "ethnicity_source_value, ethnicity_source_concept_id")

    def create_csv_line(self):
        print(f"{self.person_id}, {self.gender_concept_id}, " + 
               "{self.year_of_birth}, {self.month_of_birth}, {self.day_of_birth}, {self.birth_date_time}, " +
               "{self.race_concept_id}, {self.ethnicity_concept_id}, {self.location_id}, "
               "{self.provider_id}, {self.care_site_id}, {self.person_source_value}, " +
               "{self.gender_source_value}, {self.gender_source_concept_id}, " +
               "{self.race_source_value}, {self.race_source_concept_id}, " +
               "{self.ethnicity_source_value}, {self.ethnicity_source_concept_id}")
      

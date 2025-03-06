import unittest
import datetime

domain_dates = {
    'Measurement': {'date': ['measurement_date', 'measurement_datetime'],
                    'id': 'measurement_id'},
    'Observation': {'date': ['observation_date', 'observation_datetime'],
                    'id': 'observation_id'},
    'Condition': {'start': ['condition_start_date', 'condition_start_datetime'],
                  'end': ['condition_end_date', 'condition_end_datetime'],
                  'id': 'condition_id'},
    'Procedure': {'date': ['procedure_date', 'procedure_datetime'],
                  'id': 'procedure_occurrence_id'},
    'Drug': {'start': ['drug_exposure_start_date', 'drug_exposure_start_datetime'],
             'end': ['drug_exposure_end_date', 'drug_exposure_end_datetime'],
             'id': 'drug_exposure_id'},
}


def reconcile_visit_FK_with_specific_domain(domain: str,
                                            domain_dict: list[dict[
                                                str, None | str | float | int | datetime.datetime | datetime.date]] | None,
                                            visit_dict: list[dict[
                                                str, None | str | float | int | datetime.datetime | datetime.date]] | None):
    if visit_dict is None:
        print(f"WARNING no visits for {domain} in reconcile_visit_FK_with_specific_domain")
        return

    if domain_dict is None:
        print(f"WARNING no data for {domain} in reconcile_visit_FK_with_specific_domain")
        return

    if domain not in domain_dates:
        print(f"ERROR no metadata for domain {domain} in reconcile_visit_FK_with_specific_domain")

    # Logic for domains with only date or datetime (without start and end dates)
    if 'date' in domain_dates[domain].keys():
        # Domain with a single date or datetime field
        for thing in domain_dict:
            date_field_name = domain_dates[domain]['date'][0]
            datetime_field_name = domain_dates[domain]['date'][1]

            date_field_value = thing[date_field_name]
            if thing[datetime_field_name] is not None:
                date_field_value = thing[datetime_field_name]

            if date_field_value is not None:
                have_visit = False
                for visit in visit_dict:
                    try:
                        start_visit_date = visit['visit_start_date']
                        start_visit_datetime = visit['visit_start_datetime']
                        end_visit_date = visit['visit_end_date']
                        end_visit_datetime = visit['visit_end_datetime']

                        # 1.1 If domain has datetime, compare to visit datetime if start != end
                        if isinstance(date_field_value, datetime.datetime):
                            if start_visit_datetime != end_visit_datetime:
                                # Compare to visit datetime directly
                                if start_visit_datetime <= date_field_value <= end_visit_datetime:
                                    print(
                                        f"MATCHED visit: v_start:{start_visit_datetime} d_date:{date_field_value} v_end:{end_visit_datetime}")
                                    if not have_visit:
                                        thing['visit_occurrence_id'] = visit['visit_occurrence_id']
                                else:
                                    print(f"WARNING no matching visit for {thing[domain_dates['id']]}")

                            else:
                                # 1.2 If start and end datetime are the same, adjust visit end time to end of the day
                                end_visit_datetime_adjusted = datetime.datetime.combine(end_visit_date,
                                                                                        datetime.time(23, 59, 59))
                                if start_visit_datetime <= date_field_value <= end_visit_datetime_adjusted:
                                    print(
                                        f"MATCHED visit: v_start:{start_visit_datetime} d_date:{date_field_value} adjusted v_end:{end_visit_datetime_adjusted}")
                                    if not have_visit:
                                        thing['visit_occurrence_id'] = visit['visit_occurrence_id']
                                else:
                                    print(f"WARNING no matching visit for {thing[domain_dates['id']]}")

                        # 1.3 If domain has only date, compare to visit start and end dates
                        elif isinstance(date_field_value, datetime.date):
                            if start_visit_date <= date_field_value <= end_visit_date:
                                print(
                                    f"MATCHED visit: v_start:{start_visit_date} d_date:{date_field_value} v_end:{end_visit_date}")
                                if not have_visit:
                                    thing['visit_occurrence_id'] = visit['visit_occurrence_id']
                            else:
                                print(f"WARNING no matching visit for {thing[domain_dates['id']]}")

                    except KeyError as ke:
                        print(f"WARNING missing field \"{ke}\", in visit reconciliation")
                    except Exception as e:
                        print(f"WARNING error in visit reconciliation: {e}")
                if not have_visit:
                    print(f"WARNING wasn't able to reconcile {domain} {thing}")
                    print("")

    # Logic for domains with start and end date/datetime
    elif 'start' in domain_dates[domain].keys() and 'end' in domain_dates[domain].keys():
        # Domain with start and end date/datetime
        for thing in domain_dict:
            start_date_field_name = domain_dates[domain]['start'][0]
            start_datetime_field_name = domain_dates[domain]['start'][1]
            end_date_field_name = domain_dates[domain]['end'][0]
            end_datetime_field_name = domain_dates[domain]['end'][1]

            start_date_value = thing[start_date_field_name]
            end_date_value = thing[end_date_field_name]

            if thing[start_datetime_field_name] is not None:
                start_date_value = thing[start_datetime_field_name]

            if thing[end_datetime_field_name] is not None:
                end_date_value = thing[end_datetime_field_name]

            if start_date_value is not None and end_date_value is not None:
                have_visit = False
                for visit in visit_dict:
                    try:
                        start_visit_date = visit['visit_start_date']
                        start_visit_datetime = visit['visit_start_datetime']
                        end_visit_date = visit['visit_end_date']
                        end_visit_datetime = visit['visit_end_datetime']

                        # Adjust datetime comparisons for start and end values
                        if isinstance(start_date_value, datetime.datetime) and isinstance(end_date_value,
                                                                                          datetime.datetime):
                            if start_visit_datetime != end_visit_datetime:
                                if (
                                        (start_visit_datetime <= start_date_value <= end_visit_datetime) and
                                        (start_visit_datetime <= end_date_value <= end_visit_datetime)
                                ):
                                    print(
                                        f"MATCHED visit: v_start:{start_visit_datetime} event_start:{start_date_value} event_end:{end_date_value} v_end:{end_visit_datetime}")
                                    if not have_visit:
                                        thing['visit_occurrence_id'] = visit['visit_occurrence_id']
                            else:
                                end_visit_datetime_adjusted = datetime.datetime.combine(end_visit_date,
                                                                                        datetime.time(23, 59, 59))
                                if (
                                        (start_visit_datetime <= start_date_value <= end_visit_datetime_adjusted) and
                                        (start_visit_datetime <= end_date_value <= end_visit_datetime_adjusted)
                                ):
                                    print(
                                        f"MATCHED visit: v_start:{start_visit_datetime} event_start:{start_date_value} event_end:{end_date_value} adjusted v_end:{end_visit_datetime_adjusted}")
                                    if not have_visit:
                                        thing['visit_occurrence_id'] = visit['visit_occurrence_id']

                        # Compare with dates if datetime is not available
                        elif isinstance(start_date_value, datetime.date) and isinstance(end_date_value, datetime.date):
                            if (
                                    (start_visit_date <= start_date_value <= end_visit_date) and
                                    (start_visit_date <= end_date_value <= end_visit_date)
                            ):
                                print(
                                    f"MATCHED visit: v_start:{start_visit_date} event_start:{start_date_value} event_end:{end_date_value} v_end:{end_visit_date}")
                                if not have_visit:
                                    thing['visit_occurrence_id'] = visit['visit_occurrence_id']
                    except KeyError as ke:
                        print(f"WARNING missing field \"{ke}\", in visit reconciliation")
                    except Exception as e:
                        print(f"WARNING error in visit reconciliation: {e}")
                if not have_visit:
                    print(f"WARNING wasn't able to reconcile {domain} {thing}")
                    print("")

            else:
                print(f"ERROR no date available for visit reconciliation in domain {domain} for {thing}")

    else:
        print("ERROR: Invalid date or datetime configuration.")


class TestVisitReconciliation(unittest.TestCase):

    def setUp(self):
        """ Set up sample test data for visit and domain events """
        self.visit_dict = [
            {
                'visit_occurrence_id': 101,
                'visit_start_date': datetime.date(2023, 1, 10),
                'visit_start_datetime': datetime.datetime(2023, 1, 10, 8, 0, 0),
                'visit_end_date': datetime.date(2023, 1, 10),
                'visit_end_datetime': datetime.datetime(2023, 1, 10, 16, 0, 0)
            },
            {
                'visit_occurrence_id': 102,
                'visit_start_date': datetime.date(2023, 2, 5),
                'visit_start_datetime': datetime.datetime(2023, 2, 5, 9, 0, 0),
                'visit_end_date': datetime.date(2023, 2, 5),
                'visit_end_datetime': datetime.datetime(2023, 2, 5, 17, 0, 0)
            }
        ]

    def test_reconcile_single_date(self):
        """ Test reconciliation for domains with a single date field """
        domain_dict = [
            {'measurement_id': 201, 'measurement_date': datetime.date(2023, 1, 10), 'measurement_datetime': None},
            {'measurement_id': 202, 'measurement_date': datetime.date(2023, 2, 5), 'measurement_datetime': None},
        ]

        reconcile_visit_FK_with_specific_domain('Measurement', domain_dict, self.visit_dict)

        self.assertEqual(domain_dict[0]['visit_occurrence_id'], 101)
        self.assertEqual(domain_dict[1]['visit_occurrence_id'], 102)

    def test_reconcile_single_datetime(self):
        """ Test reconciliation for domains with a single datetime field """
        domain_dict = [
            {'observation_id': 301, 'observation_date': None,
             'observation_datetime': datetime.datetime(2023, 1, 10, 10, 0, 0)},
            {'observation_id': 302, 'observation_date': None,
             'observation_datetime': datetime.datetime(2023, 2, 5, 11, 0, 0)},
        ]

        reconcile_visit_FK_with_specific_domain('Observation', domain_dict, self.visit_dict)

        self.assertEqual(domain_dict[0]['visit_occurrence_id'], 101)
        self.assertEqual(domain_dict[1]['visit_occurrence_id'], 102)

    def test_reconcile_start_end_dates(self):
        """ Test reconciliation for domains with start and end dates """
        domain_dict = [
            {'condition_id': 401, 'condition_start_date': datetime.date(2023, 1, 10), 'condition_start_datetime': None,
             'condition_end_date': datetime.date(2023, 1, 10), 'condition_end_datetime': None},
            {'condition_id': 402, 'condition_start_date': datetime.date(2023, 2, 5), 'condition_start_datetime': None,
             'condition_end_date': datetime.date(2023, 2, 5), 'condition_end_datetime': None},
        ]

        reconcile_visit_FK_with_specific_domain('Condition', domain_dict, self.visit_dict)

        self.assertEqual(domain_dict[0]['visit_occurrence_id'], 101)
        self.assertEqual(domain_dict[1]['visit_occurrence_id'], 102)

    def test_reconcile_start_end_datetime(self):
        """ Test reconciliation for domains with start and end datetime """
        domain_dict = [
            {'drug_exposure_id': 501, 'drug_exposure_start_date': None,
             'drug_exposure_start_datetime': datetime.datetime(2023, 1, 10, 9, 0, 0),
             'drug_exposure_end_date': None, 'drug_exposure_end_datetime': datetime.datetime(2023, 1, 10, 15, 0, 0)},
            {'drug_exposure_id': 502, 'drug_exposure_start_date': None,
             'drug_exposure_start_datetime': datetime.datetime(2023, 2, 5, 10, 0, 0),
             'drug_exposure_end_date': None, 'drug_exposure_end_datetime': datetime.datetime(2023, 2, 5, 16, 0, 0)},
        ]

        reconcile_visit_FK_with_specific_domain('Drug', domain_dict, self.visit_dict)

        self.assertEqual(domain_dict[0]['visit_occurrence_id'], 101)
        self.assertEqual(domain_dict[1]['visit_occurrence_id'], 102)

    def test_no_matching_visit(self):
        """ Test case where no visit matches the domain event """
        domain_dict = [
            {'procedure_occurrence_id': 601, 'procedure_date': datetime.date(2023, 3, 15), 'procedure_datetime': None},
        ]

        reconcile_visit_FK_with_specific_domain('Procedure', domain_dict, self.visit_dict)

        self.assertIsNone(domain_dict[0].get('visit_occurrence_id'))  # Should not be assigned any visit

    def test_edge_case_visit_same_start_end_datetime(self):
        """
        Test case where visit start datetime == visit end datetime.
        The visit end datetime should be adjusted to 23:59:59 for comparison.
        """
        domain_dict = [
            {'drug_exposure_id': 701,
             'drug_exposure_start_date': None,
             'drug_exposure_start_datetime': datetime.datetime(2023, 1, 10, 23, 30, 0),
             'drug_exposure_end_date': None,
             'drug_exposure_end_datetime': datetime.datetime(2023, 1, 10, 23, 59, 59)}
        ]

        # Modify the visit data to create an edge case
        visit_dict = [
            {
                'visit_occurrence_id': 101,
                'visit_start_date': datetime.date(2023, 1, 10),
                'visit_start_datetime': datetime.datetime(2023, 1, 10, 8, 0, 0),
                'visit_end_date': datetime.date(2023, 1, 10),
                'visit_end_datetime': datetime.datetime(2023, 1, 10, 8, 0, 0)
                # Same as start, should trigger adjustment
            }
        ]

        reconcile_visit_FK_with_specific_domain('Drug', domain_dict, visit_dict)

        # The function should adjust visit_end_datetime and successfully match the visit
        self.assertEqual(domain_dict[0]['visit_occurrence_id'], 101)


if __name__ == '__main__':
    unittest.main()

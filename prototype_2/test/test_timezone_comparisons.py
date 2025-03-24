import unittest
import datetime
import pytz
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')


def normalize_datetime(dt1, dt2):
    """ Normalize datetimes before comparison.
        - If both have timezones, convert to UTC.
        - If one is naive and the other is aware, log a warning and convert the naive datetime to UTC.
    """
    if dt1.tzinfo is not None and dt2.tzinfo is not None:
        logging.info(f"Both datetime values are timezone-aware. Converting both to UTC.")
        return dt1.astimezone(pytz.UTC), dt2.astimezone(pytz.UTC)

    elif dt1.tzinfo is None and dt2.tzinfo is None:
        logging.info("Both datetime values are naive. Comparing directly.")
        return dt1, dt2

    else:
        logging.warning(f"Mixing offset-naive and offset-aware datetimes! Converting naive datetime to UTC.")
        # Convert naive datetime to UTC before comparison
        if dt1.tzinfo is None:
            dt1 = dt1.replace(tzinfo=pytz.UTC)
        if dt2.tzinfo is None:
            dt2 = dt2.replace(tzinfo=pytz.UTC)
        return dt1, dt2


class TestDatetimeComparison(unittest.TestCase):

    def setUp(self):
        """ Set up test datetimes with and without timezones """
        self.naive_dt1 = datetime.datetime(2024, 3, 1, 11, 0, 0)
        self.naive_dt2 = datetime.datetime(2024, 3, 1, 15, 0, 0)

        self.utc_dt1 = datetime.datetime(2024, 3, 1, 12, 0, 0, tzinfo=pytz.UTC)
        self.utc_dt2 = datetime.datetime(2024, 3, 1, 15, 0, 0, tzinfo=pytz.UTC)

        self.est_dt1 = datetime.datetime(2024, 3, 1, 7, 0, 0, tzinfo=pytz.timezone("US/Eastern"))
        self.est_dt2 = datetime.datetime(2024, 3, 1, 10, 0, 0, tzinfo=pytz.timezone("US/Eastern"))

    def test_compare_naive_datetimes(self):
        """ Test that naive datetimes compare normally """
        logging.info("Testing comparison of two naive datetimes.")
        dt1, dt2 = normalize_datetime(self.naive_dt1, self.naive_dt2)
        self.assertTrue(dt1 < dt2)

    def test_compare_aware_datetimes_same_tz(self):
        """ Test that UTC-aware datetimes compare correctly """
        logging.info("Testing comparison of two UTC-aware datetimes.")
        dt1, dt2 = normalize_datetime(self.utc_dt1, self.utc_dt2)
        self.assertTrue(dt1 < dt2)

    def test_compare_aware_datetimes_different_tz(self):
        """ Test that different timezone datetimes compare after conversion """
        logging.info("Testing comparison of two datetimes in different timezones (EST vs UTC).")
        dt1, dt2 = normalize_datetime(self.est_dt1, self.utc_dt2)
        self.assertTrue(dt1 < dt2)  # EST time should be converted before comparison

    def test_compare_naive_and_aware_datetime(self):
        """ Test that mixing naive and aware datetimes logs a warning but still works """
        logging.info("Testing comparison of a naive datetime and an aware datetime.")
        dt1, dt2 = normalize_datetime(self.naive_dt1, self.est_dt1)

        logging.info(self.naive_dt1)
        logging.info(self.est_dt1)
        logging.info("-----------")
        logging.info(dt1)
        logging.info(dt2)

        self.assertTrue(dt1 < dt2)


if __name__ == '__main__':
    unittest.main()
import unittest
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')


class TestDataFrameTypeHandling(unittest.TestCase):
    """ Unit tests for Pandas DataFrame type handling """

    def setUp(self):
        """ Sets up test data and expected DataFrame structure before each test """
        self.column_names = ['id', 'concept_id', 'date', 'date_time', 'quantity', 'lot_number']
        self.column_and_types = {'id': "int64", 'concept_id': "int64", 'date': "datetime64[ns]",
                                 'date_time': "datetime64[ns]", 'quantity': "float64", 'lot_number': "string"}

        # Sample valid data
        self.valid_data = [
            {'id': 1, 'concept_id': 100, 'date': '2024-02-05', 'date_time': '2024-02-05 14:30:00',
             'quantity': 5.0, 'lot_number': 'A123'},
            {'id': 2, 'concept_id': 200, 'date': '2024-02-06', 'date_time': '2024-02-06 09:15:00',
             'quantity': 10.5, 'lot_number': 'B456'}
        ]

        # Invalid data (for testing error handling)
        self.invalid_data = [
            {'id': 'X', 'concept_id': 'Invalid', 'date': 'invalid_date', 'date_time': 'invalid_datetime',
             'quantity': 'bad_data', 'lot_number': 999},
            {'id': 3, 'concept_id': 300, 'date': '2024-02-07', 'date_time': '2024-02-07 12:00:00',
             'quantity': 15.0, 'lot_number': 'C789'}  # Valid row
        ]

    def test_dataframe_with_valid_data(self):
        """ Test creating DataFrame with valid data and enforcing types """
        df = pd.DataFrame(self.valid_data, columns=self.column_names)

        # Convert date columns to datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')

        # Enforce data types
        df = df.astype(self.column_and_types)

        print(df)
        print("-------------------")

        try:
            # Assert column types
            self.assertEqual(df.dtypes['id'], "int64")
            self.assertEqual(df.dtypes['concept_id'], "int64")
            self.assertEqual(df.dtypes['date'], "datetime64[ns]")
            self.assertEqual(df.dtypes['date_time'], "datetime64[ns]")
            self.assertEqual(df.dtypes['quantity'], "float64")
            self.assertEqual(df.dtypes['lot_number'], "string")
        except AssertionError as e:
            logging.error("❌ Type mismatch detected in valid data test!")
            print(df)
            raise e

    def test_dataframe_with_invalid_data(self):
        """ Test behavior when invalid data is added """
        df = pd.DataFrame(self.invalid_data, columns=self.column_names)

        # Convert date columns to datetime (invalid values will become NaT)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')

        # Enforce data types with errors='coerce' to handle conversion failures
        df = df.astype(self.column_and_types, errors='ignore')

        try:
            # Ensure invalid values are converted to NaN (for numerical columns)
            self.assertTrue(pd.isna(df['id']).all() or df['id'].dtype == "int64")
            self.assertTrue(pd.isna(df['concept_id']).all() or df['concept_id'].dtype == "int64")
            self.assertTrue(pd.isna(df['quantity']).all() or df['quantity'].dtype == "float64")

            # Ensure invalid date fields become NaT
            self.assertTrue(pd.isna(df['date']).all())
            self.assertTrue(pd.isna(df['date_time']).all())

            # Ensure lot_number is a string even if invalid
            self.assertEqual(df.dtypes['lot_number'], "string")
        except AssertionError as e:
            logging.error("❌ Invalid data test failed: Type mismatch or unhandled NaN values!")
            print("-------------------")
            print(df)
            raise e

    def test_drop_invalid_rows(self):
        """ Test dropping rows where conversion fails """
        df = pd.DataFrame(self.valid_data + self.invalid_data, columns=self.column_names)

        # Convert date columns to datetime (invalid values will become NaT)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')

        # Convert numeric columns properly before enforcing types
        df['id'] = pd.to_numeric(df['id'], errors='coerce')  # Coerce invalid numeric values to NaN
        df['concept_id'] = pd.to_numeric(df['concept_id'], errors='coerce')
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

        # Apply astype() now that invalid values are handled
        df = df.astype(self.column_and_types, errors='ignore')

        # Identify rows that contain NaN (invalid rows)
        invalid_rows = df[df.isna().any(axis=1)]

        # Log the dropped rows
        if not invalid_rows.empty:
            logging.warning(f"Dropping {len(invalid_rows)} invalid rows due to failed type conversion:\n{invalid_rows}")

        # Drop rows where any conversion failed (NaN or NaT)
        df_cleaned = df.dropna()

        print("\n-------------------")
        print("DataFrame after dropping invalid rows:\n", df_cleaned)

        try:
            # Ensure invalid rows are removed
            self.assertEqual(df_cleaned.shape[0], 3)  # Should keep only valid rows
            self.assertNotIn('X', df_cleaned['id'].values)  # Ensure 'X' row is dropped
            self.assertNotIn('Invalid', df_cleaned['concept_id'].values)  # Ensure invalid concept_id row is dropped
        except AssertionError as e:
            logging.error("❌ Test failed: Rows with failed conversions were not dropped correctly!")
            print("DataFrame after dropping invalid rows:\n", df_cleaned)  # Print only when test fails
            raise e


if __name__ == '__main__':
    unittest.main()

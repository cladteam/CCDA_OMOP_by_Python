import unittest
import prototype_2.value_transformations as VT


       


class ValueTransformTest_concat_0 (unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_field = None
        self.second_field = None
        self.expected_output = ""

    def test_concat(self):
        args_dict = { 'first_field': self.first_field,
                      'second_field': self.second_field }
        output_string = VT.concat_fields(args_dict)
        self.assertIsNone(output_string)

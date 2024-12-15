import unittest
import prototype_2.value_transformations as VT


# Lots of context to call do_derived_fields directly. Testing here from how that
# function calls these transformation calls after it does the lookups inot ouptut_dict
# for data.
# def do_derived_fields(output_dict, root_element, root_path, domain,  domain_meta_dict, error_fields_set):

        


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


class ValueTransformTest_concat_1 (unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_field = 'a'
        self.second_field = None
        self.expected_output = "a"

    def test_concat(self):
        args_dict = { 'first_field': self.first_field,
                      'second_field': self.second_field}
        output_string = VT.concat_fields(args_dict)
        self.assertEqual(output_string, self.expected_output) 
        
class ValueTransformTest_concat_2 (unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_field = 'a'
        self.second_field = None
        self.expected_output = "a"

    def test_concat(self):
        args_dict = { 'first_field': self.first_field,
                      'second_field': self.second_field}
        output_string = VT.concat_fields(args_dict)
        self.assertEqual(output_string, self.expected_output) 
        

                         

 